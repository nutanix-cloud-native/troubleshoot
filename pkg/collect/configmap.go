package collect

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	troubleshootv1beta2 "github.com/replicatedhq/troubleshoot/pkg/apis/troubleshoot/v1beta2"
	"github.com/replicatedhq/troubleshoot/pkg/k8sutil"
	corev1 "k8s.io/api/core/v1"
	kuberneteserrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

type ConfigMapOutput struct {
	Namespace       string            `json:"namespace"`
	Name            string            `json:"name"`
	Key             string            `json:"key"`
	ConfigMapExists bool              `json:"configMapExists"`
	KeyExists       bool              `json:"keyExists"`
	Value           string            `json:"value,omitempty"`
	Data            map[string]string `json:"data,omitempty"`
}

type CollectConfigMap struct {
	Collector    *troubleshootv1beta2.ConfigMap
	BundlePath   string
	Namespace    string
	ClientConfig *rest.Config
	Client       kubernetes.Interface
	Context      context.Context
	RBACErrors
}

func (c *CollectConfigMap) Title() string {
	return getCollectorName(c)
}

func (c *CollectConfigMap) IsExcluded() (bool, error) {
	return isExcluded(c.Collector.Exclude)
}

func (c *CollectConfigMap) Collect(progressChan chan<- interface{}) (CollectorResult, error) {
	output := NewResult()

	configMaps := []corev1.ConfigMap{}
	namespace := c.Collector.Namespace

	if c.Collector.Name != "" {
		if namespace == "" {
			kubeconfig := k8sutil.GetKubeconfig()
			ns, _, err := kubeconfig.Namespace()
			klog.V(2).Infof("no namespace was set for configmap '%s': using namespace '%s' from current kubeconfig context", c.Collector.Name, ns)
			if err != nil {
				return nil, errors.Wrapf(err, "a namespace was not specified for configmap '%s' and could not be discovered from kubeconfig", c.Collector.Name)
			}
			namespace = ns
		}
		klog.V(1).Infof("looking for configmap '%s' in namespace '%s'", c.Collector.Name, namespace)
		configMap, err := c.Client.CoreV1().ConfigMaps(namespace).Get(c.Context, c.Collector.Name, metav1.GetOptions{})
		if err != nil {
			if kuberneteserrors.IsNotFound(err) {
				filePath, encoded, err := configMapToOutput(c.Collector, nil)
				if err != nil {
					return output, errors.Wrapf(err, "collect secret %s", c.Collector.Name)
				}
				output.SaveResult(c.BundlePath, filePath, bytes.NewBuffer(encoded))
			}
			output.SaveResult(c.BundlePath, GetConfigMapErrorsFileName(c.Collector), marshalErrors([]string{err.Error()}))
			return output, nil
		}
		configMaps = append(configMaps, *configMap)
	} else if len(c.Collector.Selector) > 0 {
		cms, err := listConfigMapsForSelector(c.Context, c.Client, c.Collector.Namespace, c.Collector.Selector)
		if err != nil {
			output.SaveResult(c.BundlePath, GetConfigMapErrorsFileName(c.Collector), marshalErrors([]string{err.Error()}))
			return output, nil
		}
		configMaps = append(configMaps, cms...)
	} else {
		return nil, errors.New("either name or selector must be specified")
	}

	for _, configMap := range configMaps {
		filePath, encoded, err := configMapToOutput(c.Collector, &configMap)
		if err != nil {
			return output, errors.Wrapf(err, "collect configMap %s", configMap.Name)
		}
		output.SaveResult(c.BundlePath, filePath, bytes.NewBuffer(encoded))
	}

	return output, nil
}

func configMapToOutput(configMapCollector *troubleshootv1beta2.ConfigMap, configMap *corev1.ConfigMap) (string, []byte, error) {
	foundConfigMap := ConfigMapOutput{
		Namespace: configMapCollector.Namespace,
		Name:      configMapCollector.Name,
		Key:       configMapCollector.Key,
	}

	if configMap != nil {
		foundConfigMap.ConfigMapExists = true
		foundConfigMap.Name = configMap.Name
		foundConfigMap.Namespace = configMap.Namespace
		if configMapCollector.IncludeAllData {
			foundConfigMap.Data = configMap.Data
		}
		if configMapCollector.Key != "" {
			if val, ok := configMap.Data[configMapCollector.Key]; ok {
				foundConfigMap.KeyExists = true
				if configMapCollector.IncludeValue {
					foundConfigMap.Value = string(val)
				}
			}
		}
	}

	return marshalConfigMapOutput(configMapCollector, foundConfigMap)
}

func listConfigMapsForSelector(ctx context.Context, client kubernetes.Interface, namespace string, selector []string) ([]corev1.ConfigMap, error) {
	serializedLabelSelector := strings.Join(selector, ",")

	listOptions := metav1.ListOptions{
		LabelSelector: serializedLabelSelector,
	}

	configMaps, err := client.CoreV1().ConfigMaps(namespace).List(ctx, listOptions)
	if err != nil {
		return nil, err
	}

	return configMaps.Items, nil
}

func marshalConfigMapOutput(configMapCollector *troubleshootv1beta2.ConfigMap, configMap ConfigMapOutput) (string, []byte, error) {
	path := GetConfigMapFileName(configMap.Namespace, configMap.Name, configMapCollector.Key)

	b, err := json.MarshalIndent(configMap, "", "  ")
	if err != nil {
		return path, nil, err
	}

	return path, b, nil
}

func GetConfigMapFileName(namespace, name, key string) string {
	parts := []string{"configmaps", namespace, name}
	if key != "" {
		parts = append(parts, key)
	}
	return fmt.Sprintf("%s.json", filepath.Join(parts...))
}

func GetConfigMapErrorsFileName(configMapCollector *troubleshootv1beta2.ConfigMap) string {
	parts := []string{"configmaps-errors", configMapCollector.Namespace}
	if configMapCollector.Name != "" {
		parts = append(parts, configMapCollector.Name)
	} else {
		parts = append(parts, selectorToString(configMapCollector.Selector))
	}
	if configMapCollector.Key != "" {
		parts = append(parts, configMapCollector.Key)
	}
	return fmt.Sprintf("%s.json", filepath.Join(parts...))
}
