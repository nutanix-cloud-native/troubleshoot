package supportbundle

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	troubleshootv1beta2 "github.com/replicatedhq/troubleshoot/pkg/apis/troubleshoot/v1beta2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func TestRunCollectors_SlashedCollectorTitleErrorPersistsForArchive(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && r.URL.Path == "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews" {
			_ = json.NewEncoder(w).Encode(&authorizationv1.SelfSubjectAccessReview{
				TypeMeta: metav1.TypeMeta{APIVersion: "authorization.k8s.io/v1", Kind: "SelfSubjectAccessReview"},
				Status:   authorizationv1.SubjectAccessReviewStatus{Allowed: true},
			})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(&metav1.Status{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
			Status:   metav1.StatusFailure,
			Message:  "simulated apiserver failure",
		})
	}))
	defer srv.Close()

	bundlePath := t.TempDir()
	archivePath := filepath.Join(t.TempDir(), "support-bundle.tar.gz")
	progressCh := make(chan interface{}, 32)

	collectors := []*troubleshootv1beta2.Collect{
		{
			ExecCopyFromHost: &troubleshootv1beta2.ExecCopyFromHost{
				CollectorMeta: troubleshootv1beta2.CollectorMeta{
					CollectorName: "node-diagnostics",
				},
				Name:      "node-diagnostics",
				Namespace: "nkp-diagnostics",
				Image:     "example.com/collector:v1",
			},
		},
	}

	opts := SupportBundleCreateOpts{
		CollectWithoutPermissions: true,
		KubernetesRestConfig:      &rest.Config{Host: srv.URL},
		ProgressChan:              progressCh,
		CollectorProgressCallback: func(chan interface{}, string) {},
	}

	result, err := runCollectors(context.Background(), collectors, nil, bundlePath, opts)
	require.NoError(t, err)

	errFile := filepath.Join(bundlePath, "collector-errors", "exec-copy-from-host", "node-diagnostics-error.json")
	_, statErr := os.Stat(errFile)
	require.NoError(t, statErr)

	require.NoError(t, result.ArchiveBundle(bundlePath, archivePath))
	_, archiveStatErr := os.Stat(archivePath)
	assert.NoError(t, archiveStatErr)
}

func TestGetCollectTimeout(t *testing.T) {
	t.Run("value not set returns default", func(t *testing.T) {
		opts := SupportBundleCreateOpts{}
		actualTimeout := getCollectTimeout(opts)
		if actualTimeout != defaultTimeout {
			t.Errorf("getCollectTimeout() = %d, want default %d", actualTimeout, defaultTimeout)
		}
	})

	t.Run("value set returns configured timeout", func(t *testing.T) {
		opts := SupportBundleCreateOpts{RemoteHostCollectTimeoutSeconds: 90}
		actualTimeout := getCollectTimeout(opts)
		expectedTimeout := 90
		if actualTimeout != expectedTimeout {
			t.Errorf("getCollectTimeout() = %d, want %d", actualTimeout, expectedTimeout)
		}
	})
}
