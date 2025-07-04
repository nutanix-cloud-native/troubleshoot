package collect

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/ksuid"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kuberneteserrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"

	troubleshootv1beta2 "github.com/replicatedhq/troubleshoot/pkg/apis/troubleshoot/v1beta2"
	"github.com/replicatedhq/troubleshoot/pkg/k8sutil"
)

// Exec and copy from host exec collector is a collector that executes a
// container with configured linux permissions on nodes in the cluster based on
// the provided selector and copies the results produced by the container to the
// support bundle.

// The k8s has no primitive for running a dameonset like job that would run once
// on all nodes in the cluster. To overcome this issue this collector runs
// container that collects data as an `initContainer` and it expects that the
// container runs one-off process with an expectation of a clean exit. Once the
// initContainer is completed an additional `pause` container is launched that
// is a no-op container. This container only waits and allows the main
// support-bundle process to collect produced data (copy from the container).
// The `initContainer` and `pause` containers share a volume to which the
// container producing the support bundle data writes. Data written to other
// paths by the `initContainer` will not be copied back to the support bundle.

// This collector is a combination of `CopyFromHost` and `Exec` containers.

const (
	// execCopyFromHostSharedVolumePath is a path to a directory that is
	// shared between the init container and pause container. The container that
	// is producing data that should be copied to the diagnostics bundle should
	// write data to this path.
	execCopyFromHostSharedVolumePath = "/host/output"

	// execCopyFromHostHostVolumePath is a path that is mounted to the container
	// that is collecting data from the host node.
	execCopyFromHostHostVolumePath = "/host"

	// defaultPauseImage is the name of the image that will be launched to transfer
	// data collected by the collector container.
	defaultPauseImage = "mesosphere/pause-alpine:3.2"
)

type CollectExecCopyFromHost struct {
	Collector    *troubleshootv1beta2.ExecCopyFromHost
	BundlePath   string
	Namespace    string
	ClientConfig *rest.Config
	Client       kubernetes.Interface
	Context      context.Context
	RBACErrors
}

func (c *CollectExecCopyFromHost) Title() string {
	return getCollectorName(c)
}

func (c *CollectExecCopyFromHost) IsExcluded() (bool, error) {
	return isExcluded(c.Collector.Exclude)
}

func (c *CollectExecCopyFromHost) Collect(progressChan chan<- interface{}) (CollectorResult, error) {
	labels := map[string]string{
		"app.kubernetes.io/managed-by":        "troubleshoot.sh",
		"troubleshoot.sh/collector":           "execcopyfromhost",
		"troubleshoot.sh/execcopyfromhost-id": ksuid.New().String(),
	}
	// here we use the namespace in the bundle configurations, if any is provided,
	// this is to avoid having to verify pods, potentially failing, when namespaces
	// are labeled, or a cluster-wide defualt is set, to enforce a podSecurity admission mode
	// other than privileged
	namespace := c.Collector.Namespace
	if namespace == "" {
		namespace = c.Namespace
	}
	if namespace == "" {
		kubeconfig := k8sutil.GetKubeconfig()
		namespace, _, _ = kubeconfig.Namespace()
	}

	_, cleanup, err := execCopyFromHostCreateDaemonSet(
		c.Context, c.Client, c.Collector, namespace, "troubleshoot-execcopyfromhost-", labels,
	)
	defer cleanup()
	if err != nil {
		return nil, errors.Wrap(err, "create daemonset")
	}

	childCtx, cancel := context.WithCancel(c.Context)
	if c.Collector.Timeout != "" {
		timeout, err := time.ParseDuration(c.Collector.Timeout)
		if err != nil {
			return nil, errors.Wrap(err, "parse timeout")
		}

		if timeout > 0 {
			childCtx, cancel = context.WithTimeout(childCtx, timeout)
		}
	}
	defer cancel()

	errCh := make(chan error, 1)
	resultCh := make(chan map[string][]byte, 1)
	go func() {
		var outputPath string
		if c.Collector.Name != "" {
			outputPath = c.Collector.Name
		} else {
			outputPath = labels["troubleshoot.sh/execcopyfromhost-id"]
		}
		b, err := c.execCopyFromHostGetFilesFromPods(
			childCtx, c.ClientConfig, c.Client, c.Collector,
			outputPath, labels, namespace,
		)
		if err != nil {
			errCh <- err
		} else {
			resultCh <- b
		}
	}()

	select {
	case <-childCtx.Done():
		return nil, errors.New("timeout")
	case result := <-resultCh:
		return result, nil
	case err := <-errCh:
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, errors.New("timeout")
		}
		return nil, err
	}
}

func execCopyFromHostCreateDaemonSet(
	ctx context.Context,
	client kubernetes.Interface,
	collector *troubleshootv1beta2.ExecCopyFromHost,
	namespace string,
	generateName string,
	labels map[string]string,
) (string, func(), error) {
	pullPolicy := corev1.PullIfNotPresent
	if collector.ImagePullPolicy != "" {
		pullPolicy = corev1.PullPolicy(collector.ImagePullPolicy)
	}
	hostPathVolumeType := corev1.HostPathDirectory

	// Create list of capabilities
	capabilities := corev1.Capabilities{
		Add: []corev1.Capability{},
	}
	for _, capability := range collector.Capabilities {
		capabilities.Add = append(capabilities.Add, corev1.Capability(capability))
	}

	pauseImage := defaultPauseImage
	if collector.PauseImage != "" {
		pauseImage = collector.PauseImage
	}

	ds := appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: generateName,
			Namespace:    namespace,
			Labels:       labels,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyAlways,
					HostNetwork:   collector.HostNetwork,
					HostPID:       collector.HostPID,
					HostIPC:       collector.HostIPC,
					InitContainers: []corev1.Container{
						{
							Name:            "collector",
							Image:           collector.Image,
							ImagePullPolicy: pullPolicy,
							Command:         collector.Command,
							Args:            collector.Args,
							WorkingDir:      collector.WorkingDir,
							SecurityContext: &corev1.SecurityContext{
								Capabilities: &capabilities,
								Privileged:   &collector.Privileged,
								RunAsUser:    pointer.Int64Ptr(collector.RunAsUser),
								RunAsNonRoot: pointer.BoolPtr(collector.RunAsNonRoot),
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "host",
									MountPath: execCopyFromHostHostVolumePath,
								},
								{
									Name:      "output",
									MountPath: execCopyFromHostSharedVolumePath,
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Image:           pauseImage,
							ImagePullPolicy: pullPolicy,
							Name:            "pause",
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "output",
									MountPath: "/data",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "host",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/",
									Type: &hostPathVolumeType,
								},
							},
						},
						{
							Name: "output",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
		},
	}

	// TODO(mh): Add support for failing nodes and expose tollerations via
	// the configuration? This could be useful to attempt running diagnostics
	// collection on nodes with issues like memory pressure.
	if collector.IncludeControlPlane {
		ds.Spec.Template.Spec.Tolerations = []corev1.Toleration{
			{
				Effect:   corev1.TaintEffectNoSchedule,
				Key:      "node-role.kubernetes.io/master",
				Operator: corev1.TolerationOpExists,
			},
			{
				Key:      "node-role.kubernetes.io/control-plane",
				Operator: "Exists",
				Effect:   "NoSchedule",
			},
		}
	}

	cleanupFuncs := []func(){}
	cleanup := func() {
		for _, fn := range cleanupFuncs {
			fn()
		}
	}

	if collector.ImagePullSecret != nil && collector.ImagePullSecret.Data != nil {
		secretName, err := createSecret(ctx, client, namespace, collector.ImagePullSecret)
		if err != nil {
			return "", cleanup, errors.Wrap(err, "create secret")
		}
		ds.Spec.Template.Spec.ImagePullSecrets = append(ds.Spec.Template.Spec.ImagePullSecrets, corev1.LocalObjectReference{Name: secretName})

		cleanupFuncs = append(cleanupFuncs, func() {
			err := client.CoreV1().Secrets(namespace).Delete(ctx, collector.ImagePullSecret.Name, metav1.DeleteOptions{})
			if err != nil && !kuberneteserrors.IsNotFound(err) {
				klog.Infof("Failed to delete secret %s: %v", collector.ImagePullSecret.Name, err)
			}
		})
	}

	createdDS, err := client.AppsV1().DaemonSets(namespace).Create(ctx, &ds, metav1.CreateOptions{})
	if err != nil {
		return "", cleanup, errors.Wrap(err, "create daemonset")
	}
	cleanupFuncs = append(cleanupFuncs, func() {
		if err := client.AppsV1().DaemonSets(namespace).Delete(ctx, createdDS.Name, metav1.DeleteOptions{}); err != nil {
			klog.Infof("Failed to delete daemonset %s: %v", createdDS.Name, err)
		}
	})

	timeout := 30 * time.Second
	if collector.DaemonSetTimeout != "" {
		timeout, err = time.ParseDuration(collector.DaemonSetTimeout)
		if err != nil {
			return "", cleanup, errors.Wrap(err, "parse daemonsettimeout")
		}
	}

	// This timeout is different from collector timeout.
	// Time it takes to pull images should not count towards collector timeout.
	childCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		select {
		case <-time.After(1 * time.Second):
		case <-childCtx.Done():
			return createdDS.Name, cleanup, errors.Wrap(ctx.Err(), "wait for daemonset")
		}

		ds, err := client.AppsV1().DaemonSets(namespace).Get(ctx, createdDS.Name, metav1.GetOptions{})
		if err != nil {
			if !kuberneteserrors.IsNotFound(err) {
				continue
			}
			return createdDS.Name, cleanup, errors.Wrap(err, "get daemonset")
		}

		if ds.Status.DesiredNumberScheduled == 0 || ds.Status.DesiredNumberScheduled != ds.Status.NumberAvailable {
			continue
		}

		break
	}

	return createdDS.Name, cleanup, nil
}

func (c *CollectExecCopyFromHost) execCopyFromHostGetFilesFromPods(
	ctx context.Context,
	clientConfig *restclient.Config,
	client kubernetes.Interface,
	collector *troubleshootv1beta2.ExecCopyFromHost,
	outputPath string,
	labelSelector map[string]string,
	namespace string,
) (CollectorResult, error) {
	opts := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labelSelector).String(),
	}

	pods, err := client.CoreV1().Pods(namespace).List(ctx, opts)
	if err != nil {
		return nil, errors.Wrap(err, "list pods")
	}

	clientSet, err := kubernetes.NewForConfig(c.ClientConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create k8s clientset")
	}

	output := NewResult()
	for _, pod := range pods.Items {

		outputNodePath := filepath.Join(outputPath, pod.Spec.NodeName)
		files, stderr, err := copyFilesFromPod(
			ctx, filepath.Join(c.BundlePath, outputNodePath), clientConfig,
			client, pod.Name, "pause", namespace, "/data", collector.ExtractArchive,
		)

		if err != nil {

			// Save error message from the copy operation
			msg := fmt.Sprintf("[%s] failed to copy data from the pod container pause: %s", time.Now().String(), err.Error())
			output.SaveResult(c.BundlePath, filepath.Join(outputNodePath, "file-copy-error.txt"), bytes.NewReader([]byte(msg)))
			if len(stderr) > 0 {
				output.SaveResult(c.BundlePath, filepath.Join(outputNodePath, "stderr.txt"), bytes.NewBuffer(stderr))
			}

			// Save pod status
			podJson, err := json.MarshalIndent(pod, "", "  ")
			if err != nil {
				timestampedErr := fmt.Sprintf("[%s] %s", time.Now().String(), err.Error())
				output.SaveResult(c.BundlePath, filepath.Join(outputNodePath, "pod-collector-marshall-error.txt"), bytes.NewReader([]byte(timestampedErr)))
			} else {
				output.SaveResult(c.BundlePath, filepath.Join(outputNodePath, "pod-collector.json"), bytes.NewReader(podJson))
			}

			// Attempt to get a stdout and data from the collector initcontainer
			for _, initContainer := range pod.Spec.InitContainers {
				logPath := filepath.Join(outputNodePath, fmt.Sprintf("pod-%s", initContainer.Name))
				copyContainerLogsResult, copyErr := copyContainerLogs(ctx, c.BundlePath, clientSet, pod, initContainer.Name, logPath, false)
				if copyErr != nil {
					timestampedCopyErr := fmt.Sprintf("[%s] %s", time.Now().String(), copyErr.Error())
					output.SaveResult(c.BundlePath, fmt.Sprintf("%s-log-copy-error.txt", logPath), bytes.NewReader([]byte(timestampedCopyErr)))
				} else {
					copyResultsToOutput(output, copyContainerLogsResult)
				}

				filesCopiedFromCollector, _, err := copyFilesFromPod(
					ctx, filepath.Join(c.BundlePath, outputNodePath), clientConfig,
					client, pod.Name, initContainer.Name, namespace, execCopyFromHostSharedVolumePath, collector.ExtractArchive,
				)
				if err != nil {
					timestampedErr := fmt.Sprintf("[%s] %s", time.Now().String(), err.Error())
					output.SaveResult(c.BundlePath, fmt.Sprintf("%s-files-copy-error.txt", logPath), bytes.NewReader([]byte(timestampedErr)))
				} else {
					for k, v := range filesCopiedFromCollector {
						relPath, err := filepath.Rel(c.BundlePath, filepath.Join(c.BundlePath, filepath.Join(outputNodePath, k)))
						if err == nil {
							output[relPath] = v
						}
					}
				}
			}
		}

		for k, v := range files {
			relPath, err := filepath.Rel(c.BundlePath, filepath.Join(c.BundlePath, filepath.Join(outputNodePath, k)))
			if err != nil {
				return nil, errors.Wrap(err, "relative path")
			}
			output[relPath] = v
		}
	}

	return output, nil
}

func copyResultsToOutput(dst, src CollectorResult) {
	for k, v := range src {
		dst[k] = v
	}
}

func copyContainerLogs(
	ctx context.Context,
	bundlePath string,
	client *kubernetes.Clientset,
	pod corev1.Pod,
	container string,
	logPath string,
	previous bool,
) (CollectorResult, error) {
	podLogOpts := corev1.PodLogOptions{
		Container: container,
		Follow:    false,
		Previous:  previous,
	}
	result := NewResult()

	req := client.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &podLogOpts)
	podLogs, err := req.Stream(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get log stream")
	}
	defer podLogs.Close()

	logWriter, err := result.GetWriter(bundlePath, logPath+".log")
	if err != nil {
		return nil, errors.Wrap(err, "failed to get log writer")
	}
	defer result.CloseWriter(bundlePath, logPath+".log", logWriter)

	_, err = io.Copy(logWriter, podLogs)
	if err != nil {
		return nil, errors.Wrap(err, "failed to copy log")
	}

	return result, nil
}
