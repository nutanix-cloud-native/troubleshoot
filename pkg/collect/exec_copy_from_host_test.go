package collect

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	troubleshootv1beta2 "github.com/replicatedhq/troubleshoot/pkg/apis/troubleshoot/v1beta2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func buildTarArchive(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, body := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,
			Name:     name,
			Mode:     0o644,
			Size:     int64(len(body)),
		}))
		_, err := tw.Write([]byte(body))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

func writeWSFrame(w io.Writer, opcode byte, payload []byte) error {
	header := []byte{0x80 | opcode}
	switch n := len(payload); {
	case n < 126:
		header = append(header, byte(n))
	case n <= 0xffff:
		header = append(header, 126, 0, 0)
		binary.BigEndian.PutUint16(header[2:], uint16(n))
	default:
		header = append(header, 127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(header[2:], uint64(n))
	}
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func servePodExecTarOverWS(t *testing.T, w http.ResponseWriter, r *http.Request, tarPayload []byte) {
	t.Helper()
	wsKey := r.Header.Get("Sec-WebSocket-Key")
	require.NotEmpty(t, wsKey)

	sum := sha1.Sum([]byte(wsKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	accept := base64.StdEncoding.EncodeToString(sum[:])

	hj, ok := w.(http.Hijacker)
	require.True(t, ok)
	conn, bufrw, err := hj.Hijack()
	require.NoError(t, err)
	defer conn.Close()

	_, err = bufrw.WriteString(
		"HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + accept + "\r\n" +
			"Sec-WebSocket-Protocol: v5.channel.k8s.io\r\n\r\n",
	)
	require.NoError(t, err)
	require.NoError(t, bufrw.Flush())

	require.NoError(t, writeWSFrame(conn, 0x2, append([]byte{1}, tarPayload...)))

	statusBytes, err := json.Marshal(metav1.Status{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
		Status:   metav1.StatusSuccess,
	})
	require.NoError(t, err)
	require.NoError(t, writeWSFrame(conn, 0x2, append([]byte{3}, statusBytes...)))
	require.NoError(t, writeWSFrame(conn, 0x8, []byte{0x03, 0xe8}))

	_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, _ = io.Copy(io.Discard, conn)
}

func TestCollectExecCopyFromHost_MountsOutputOutsideHostAndPreservesPartialResultsOnFailure(t *testing.T) {
	var (
		mu          sync.Mutex
		createdDS   appsv1.DaemonSet
		execQueries []url.Values
	)

	healthyTar := buildTarArchive(t, map[string]string{
		"data/os_release": "ID=kairos\n",
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/exec") && strings.Contains(r.URL.Path, "/pod-healthy/") {
			mu.Lock()
			execQueries = append(execQueries, r.URL.Query())
			mu.Unlock()
			servePodExecTarOverWS(t, w, r, healthyTar)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/apis/apps/v1/namespaces/nkp-diagnostics/daemonsets":
			var ds appsv1.DaemonSet
			require.NoError(t, json.NewDecoder(r.Body).Decode(&ds))
			ds.Name = ds.GenerateName + "test"
			ds.Status = appsv1.DaemonSetStatus{
				DesiredNumberScheduled: 2,
				NumberAvailable:        1,
			}
			mu.Lock()
			createdDS = ds
			mu.Unlock()
			require.NoError(t, json.NewEncoder(w).Encode(&ds))

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/apis/apps/v1/namespaces/nkp-diagnostics/daemonsets/"):
			mu.Lock()
			ds := createdDS
			mu.Unlock()
			require.NoError(t, json.NewEncoder(w).Encode(&ds))

		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/apis/apps/v1/namespaces/nkp-diagnostics/daemonsets/"):
			require.NoError(t, json.NewEncoder(w).Encode(&metav1.Status{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
				Status:   metav1.StatusSuccess,
			}))

		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/nkp-diagnostics/pods":
			mu.Lock()
			podLabels := createdDS.Spec.Template.Labels
			initContainers := createdDS.Spec.Template.Spec.InitContainers
			mu.Unlock()
			podList := corev1.PodList{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"},
				Items: []corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod-healthy",
							Namespace: "nkp-diagnostics",
							Labels:    podLabels,
						},
						Spec: corev1.PodSpec{
							NodeName:       "metal-cp-1",
							InitContainers: initContainers,
						},
						Status: corev1.PodStatus{
							Phase: corev1.PodRunning,
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod-failing",
							Namespace: "nkp-diagnostics",
							Labels:    podLabels,
						},
						Spec: corev1.PodSpec{
							NodeName:       "metal-worker-1",
							InitContainers: initContainers,
						},
						Status: corev1.PodStatus{
							Phase: corev1.PodPending,
						},
					},
				},
			}
			require.NoError(t, json.NewEncoder(w).Encode(&podList))

		case strings.HasSuffix(r.URL.Path, "/exec"):
			mu.Lock()
			execQueries = append(execQueries, r.URL.Query())
			mu.Unlock()
			w.WriteHeader(http.StatusBadRequest)
			require.NoError(t, json.NewEncoder(w).Encode(&metav1.Status{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
				Status:   metav1.StatusFailure,
				Message:  "container not running: CreateContainerError: read-only file system",
				Code:     http.StatusBadRequest,
			}))

		case strings.HasSuffix(r.URL.Path, "/log"):
			w.WriteHeader(http.StatusBadRequest)
			require.NoError(t, json.NewEncoder(w).Encode(&metav1.Status{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
				Status:   metav1.StatusFailure,
				Message:  "container collector is waiting to start: CreateContainerError",
				Code:     http.StatusBadRequest,
			}))

		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	clientCfg := &rest.Config{
		Host:          srv.URL,
		ContentConfig: rest.ContentConfig{ContentType: "application/json"},
	}
	client, err := kubernetes.NewForConfig(clientCfg)
	require.NoError(t, err)

	bundleDir := t.TempDir()
	collector := &CollectExecCopyFromHost{
		BundlePath:   bundleDir,
		Namespace:    "nkp-diagnostics",
		ClientConfig: clientCfg,
		Client:       client,
		Context:      context.Background(),
		Collector: &troubleshootv1beta2.ExecCopyFromHost{
			Name:             "node-diagnostics",
			Image:            "mesosphere/dkp-diagnostics-node-collector:test",
			DaemonSetTimeout: "50ms",
			Timeout:          "5s",
			ExtractArchive:   true,
			Privileged:       true,
		},
	}

	progressCh := make(chan interface{}, 10)
	result, collectErr := collector.Collect(progressCh)

	mu.Lock()
	ds := createdDS
	queries := append([]url.Values(nil), execQueries...)
	mu.Unlock()

	require.Len(t, ds.Spec.Template.Spec.InitContainers, 1)
	initMounts := map[string]string{}
	for _, vm := range ds.Spec.Template.Spec.InitContainers[0].VolumeMounts {
		initMounts[vm.Name] = vm.MountPath
		assert.False(t, strings.HasPrefix(vm.MountPath, "/host/"), "volume %q must not mount beneath /host: %s", vm.Name, vm.MountPath)
	}
	assert.Equal(t, "/host", initMounts["host"])
	assert.Equal(t, "/output", initMounts["output"])

	require.Len(t, ds.Spec.Template.Spec.Containers, 1)
	require.Len(t, ds.Spec.Template.Spec.Containers[0].VolumeMounts, 1)
	assert.Equal(t, "output", ds.Spec.Template.Spec.Containers[0].VolumeMounts[0].Name)
	assert.Equal(t, "/data", ds.Spec.Template.Spec.Containers[0].VolumeMounts[0].MountPath)

	var collectorFallbackCmd []string
	for _, q := range queries {
		if q.Get("container") == "collector" {
			collectorFallbackCmd = q["command"]
			break
		}
	}
	assert.Equal(t, []string{"tar", "-C", "/", "-cf", "-", "output"}, collectorFallbackCmd)

	require.Error(t, collectErr)
	assert.Contains(t, collectErr.Error(), "failed to copy data from pods:")
	assert.Contains(t, collectErr.Error(), "metal-worker-1")
	assert.NotContains(t, collectErr.Error(), "metal-cp-1")
	require.NotNil(t, result)
	assert.Contains(t, result, "node-diagnostics/metal-cp-1/data/os_release")
	osReleaseBytes, err := os.ReadFile(filepath.Join(bundleDir, "node-diagnostics/metal-cp-1/data/os_release"))
	require.NoError(t, err)
	assert.Equal(t, "ID=kairos\n", string(osReleaseBytes))
	assert.Contains(t, result, "node-diagnostics/metal-worker-1/file-copy-error.txt")
	assert.Contains(t, result, "node-diagnostics/metal-worker-1/pod-collector.json")
}

func TestCollectExecCopyFromHost_AllPodsHealthyReturnsNilError(t *testing.T) {
	var (
		mu        sync.Mutex
		createdDS appsv1.DaemonSet
	)

	healthyTar := buildTarArchive(t, map[string]string{
		"data/os_release": "ID=kairos\n",
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/exec") {
			servePodExecTarOverWS(t, w, r, healthyTar)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/apis/apps/v1/namespaces/nkp-diagnostics/daemonsets":
			var ds appsv1.DaemonSet
			require.NoError(t, json.NewDecoder(r.Body).Decode(&ds))
			ds.Name = ds.GenerateName + "test"
			ds.Status = appsv1.DaemonSetStatus{
				DesiredNumberScheduled: 1,
				NumberAvailable:        1,
			}
			mu.Lock()
			createdDS = ds
			mu.Unlock()
			require.NoError(t, json.NewEncoder(w).Encode(&ds))

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/apis/apps/v1/namespaces/nkp-diagnostics/daemonsets/"):
			mu.Lock()
			ds := createdDS
			mu.Unlock()
			require.NoError(t, json.NewEncoder(w).Encode(&ds))

		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/apis/apps/v1/namespaces/nkp-diagnostics/daemonsets/"):
			require.NoError(t, json.NewEncoder(w).Encode(&metav1.Status{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
				Status:   metav1.StatusSuccess,
			}))

		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/nkp-diagnostics/pods":
			mu.Lock()
			podLabels := createdDS.Spec.Template.Labels
			initContainers := createdDS.Spec.Template.Spec.InitContainers
			mu.Unlock()
			podList := corev1.PodList{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"},
				Items: []corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod-healthy",
							Namespace: "nkp-diagnostics",
							Labels:    podLabels,
						},
						Spec: corev1.PodSpec{
							NodeName:       "metal-cp-1",
							InitContainers: initContainers,
						},
						Status: corev1.PodStatus{
							Phase: corev1.PodRunning,
						},
					},
				},
			}
			require.NoError(t, json.NewEncoder(w).Encode(&podList))

		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	clientCfg := &rest.Config{
		Host:          srv.URL,
		ContentConfig: rest.ContentConfig{ContentType: "application/json"},
	}
	client, err := kubernetes.NewForConfig(clientCfg)
	require.NoError(t, err)

	bundleDir := t.TempDir()
	collector := &CollectExecCopyFromHost{
		BundlePath:   bundleDir,
		Namespace:    "nkp-diagnostics",
		ClientConfig: clientCfg,
		Client:       client,
		Context:      context.Background(),
		Collector: &troubleshootv1beta2.ExecCopyFromHost{
			Name:             "node-diagnostics",
			Image:            "mesosphere/dkp-diagnostics-node-collector:test",
			DaemonSetTimeout: "5s",
			Timeout:          "5s",
			ExtractArchive:   true,
			Privileged:       true,
		},
	}

	progressCh := make(chan interface{}, 10)
	result, collectErr := collector.Collect(progressCh)

	require.NoError(t, collectErr)
	assert.Contains(t, result, "node-diagnostics/metal-cp-1/data/os_release")
}
