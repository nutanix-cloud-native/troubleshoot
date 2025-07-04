# D2iQ fork of Troubleshoot

This is a fork of upstream project https://github.com/replicatedhq/troubleshoot
that allows faster iteration and customizations for diagnostics data collection
project - `dkp-diagnostics`.

Any patch provided to D2iQ fork should be proposed to the upstream project which
would eventually allow using upstream in the `dkp-diagnostics` project.

The fork is based on the upstream tag `v0.26.0`.

## Changes

The list of changes compared to the upstream project.

### `ExecCopyFromHost` collector

This is a new collector created specifically for gathering host level information
from cluster nodes. The collector allows to run a provided container image in a
privileged mode, as a root user, with additional linux capabilities and with the
host filesystem mounted into the container.

This allows to collect host level information other than copying host level
files that is already possible with `CopyFromHost` collector. Similarly to the
`CopyFromHost` collector the collector runs as a Kubernetes `DaemonSet` executed
on all nodes in the system. The data that are produced by the container are
copied from pre-defined directory into the diagnostics bundle under each
node name. The name of the parent directory in the diagnostics bundle is determined
by the name of the collector specified in its configuration.

The data written into diagnostics bundle look like:

```
<collector-name> / <node-name> / data / (file1|file2|...)
```

Example of a configuration:

```
spec:
  collectors:
    - execCopyFromHost:
        name: node-diagnostics
        image: mesosphere/dkp-diagnostics-node-collector:latest
        timeout: 30s
        command:
          - "/bin/bash"
          - "-c"
          - "/diagnostics/container.sh --hostroot /host --hostpath ${PATH} --outputroot /output"
        workingDir: "/diagnostics"
        includeControlPlane: true
        privileged: true
        capabilities:
          - AUDIT_CONTROL
          - AUDIT_READ
          - BLOCK_SUSPEND
          - BPF
          - CHECKPOINT_RESTORE
          - DAC_READ_SEARCH
          - IPC_LOCK
          - IPC_OWNER
          - LEASE
          - LINUX_IMMUTABLE
          - MAC_ADMIN
          - MAC_OVERRIDE
          - NET_ADMIN
          - NET_BROADCAST
          - PERFMON
          - SYS_ADMIN
          - SYS_BOOT
          - SYS_MODULE
          - SYS_NICE
          - SYS_PACCT
          - SYS_PTRACE
          - SYS_RAWIO
          - SYS_RESOURCE
          - SYS_TIME
          - SYS_TTY_CONFIG
          - SYSLOG
          - WAKE_ALARM
        extractArchive: true
```

Example of the data produced by running this collector:

```
├── node-diagnostics
│   ├── troubleshoot-control-plane
│   │   └── data
│   │       ├── certs_expiration_kubeadm
│   │       ├── containerd_config.toml
                ...
│   │       └── whoami_validate
│   └── troubleshoot-worker
│       └── data
│           ├── containerd_config.toml
│           ├── containers_crictl
                ...
│           └── whoami_validate
```

In case of any errors while collecting the node diagnostics, `node-diagnostics/<node>/pod-collector.json` contains serialized JSON representation of running pod which can help debug why collection has failed. `node-diagnostics/<node>/pod-collector.log` file contains stdout from the collector container that runs diagnostics script.

For more information about the configuration options see the
`ExecCopyFromHost` in the `pkg/apis/troubleshoot/v1beta2/exec_copy_from_host.go`
file.


### `AllLogs` collector
This is a new collector created specifically for gathering pod logs from provided
namespaces(or from all namespaces if those are not specified).

This allows to us to collect logs of all the pods from all the namespaces.
The pod logs are collected under `allPodLogs` directory.

The data written into diagnostics bundle look like:

```
<collector-name> / <namespace-name> / <pod-name> - (container1|container2|...)
```

#### Example configuration

To collect logs from all pods from all namespaces:

```yaml
spec:
  collectors:
    - allLogs:
        namespaces:
          - "*"
```

To collect logs from all pods from selective namespaces:

```yaml
spec:
  collectors:
    - allLogs:
        namespaces:
          - default
          - dev
          - prod
```

Example of the data produced by running first collector:

```
allPodLogs
├── default
│   ├── nginx-deploy-8588f9dfb-72vj8-nginx.log
│   └── nginx-deploy-8588f9dfb-shndw-nginx.log
├── dev
│   ├── elastic-dev-es-default-0-elastic-internal-init-filesystem.log
│   ├── elastic-dev-es-default-0-elasticsearch.log
├── elastic-system
│   ├── elastic-operator-0-manager.log
│   └── elastic-operator-0-manager-previous.log
├── kube-system
│   ├── coredns-558bd4d5db-4znf9-coredns.log
│   ├── coredns-558bd4d5db-xhv9l-coredns.log
│   ├── etcd-kind-control-plane-etcd.log
│   ├── kindnet-fcpjh-kindnet-cni.log
│   ├── kube-apiserver-kind-control-plane-kube-apiserver.log
│   ├── kube-controller-manager-kind-control-plane-kube-controller-manager.log
│   ├── kube-proxy-7nqtq-kube-proxy.log
│   ├── kube-proxy-7nqtq-kube-proxy-previous.log
│   ├── kube-scheduler-kind-control-plane-kube-scheduler.log
└── local-path-storage
    └── local-path-provisioner-547f784dff-d7t5g-local-path-provisioner.log
```

For more information about the configuration options see `config/crds/troubleshoot.sh_collectors.yaml` & `pkg/collect/all_logs.go`
files.

### Support for collecting from all namespaces for `ConfigMap` and `Secret` collector

In the original collectors `namespace` is a required parameter. This adds support for collecting from all namespaces by not setting the `namespace` (or setting it to `""`).
Note: To collect all config maps / secrets an empty selector must be used (`selector: [""]`).

### Support for optional support-bundle name prefix

When generating a support bundle, it is useful to allow for providing naming defaults for providing deterministic bundle identifiers. This feature is especially useful for our convenience extension of providing diagnostics for both, a bootstrap- as well as a Konvoy or other K8s cluster. An empty prefix means nothing changes and the original naming is kept.