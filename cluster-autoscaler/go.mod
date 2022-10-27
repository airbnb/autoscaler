module k8s.io/autoscaler/cluster-autoscaler

go 1.16

require (
	cloud.google.com/go v0.65.0
	github.com/Azure/azure-sdk-for-go v43.0.0+incompatible
	github.com/Azure/go-autorest/autorest v0.11.12
	github.com/Azure/go-autorest/autorest/adal v0.9.5
	github.com/Azure/go-autorest/autorest/date v0.3.0
	github.com/Azure/go-autorest/autorest/to v0.2.0
	github.com/aws/aws-sdk-go v1.35.24
	github.com/digitalocean/godo v1.27.0
	github.com/ghodss/yaml v1.0.0
	github.com/golang/mock v1.4.4
	github.com/jmespath/go-jmespath v0.4.0
	github.com/json-iterator/go v1.1.10
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/satori/go.uuid v1.2.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519
	golang.org/x/oauth2 v0.0.0-20210819190943-2bc19b11175f
	google.golang.org/api v0.30.0
	google.golang.org/grpc v1.40.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/gcfg.v1 v1.2.0
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.21.0
	k8s.io/apimachinery v0.21.0
	k8s.io/apiserver v0.21.0
	k8s.io/client-go v0.21.0
	k8s.io/cloud-provider v0.21.0
	k8s.io/component-base v0.21.0
	k8s.io/component-helpers v0.21.0
	k8s.io/klog/v2 v2.8.0
	k8s.io/kubelet v0.0.0
	k8s.io/kubernetes v1.21.0
	k8s.io/legacy-cloud-providers v0.0.0
	k8s.io/utils v0.0.0-20201110183641-67b214c5f920
)

replace github.com/digitalocean/godo => github.com/digitalocean/godo v1.27.0

replace github.com/rancher/go-rancher => github.com/rancher/go-rancher v0.1.0

replace k8s.io/api => k8s.io/api v0.21.0

replace k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.21.0

replace k8s.io/apimachinery => k8s.io/apimachinery v0.21.1-rc.0

replace k8s.io/apiserver => k8s.io/apiserver v0.21.0

replace k8s.io/cli-runtime => k8s.io/cli-runtime v0.21.0

replace k8s.io/client-go => k8s.io/client-go v0.21.0

replace k8s.io/cloud-provider => k8s.io/cloud-provider v0.21.0

replace k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.21.0

replace k8s.io/code-generator => k8s.io/code-generator v0.21.1-rc.0

replace k8s.io/component-base => k8s.io/component-base v0.21.0

replace k8s.io/component-helpers => k8s.io/component-helpers v0.21.0

replace k8s.io/controller-manager => k8s.io/controller-manager v0.21.0

replace k8s.io/cri-api => k8s.io/cri-api v0.21.1-rc.0

replace k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.21.0

replace k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.21.0

replace k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.21.0

replace k8s.io/kube-proxy => k8s.io/kube-proxy v0.21.0

replace k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.21.0

replace k8s.io/kubectl => k8s.io/kubectl v0.21.0

replace k8s.io/kubelet => k8s.io/kubelet v0.21.0

replace k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.21.0

replace k8s.io/metrics => k8s.io/metrics v0.21.0

replace k8s.io/mount-utils => k8s.io/mount-utils v0.21.1-rc.0

replace k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.21.0

replace k8s.io/sample-cli-plugin => k8s.io/sample-cli-plugin v0.21.0

replace k8s.io/sample-controller => k8s.io/sample-controller v0.21.0

replace go.etcd.io/etcd => go.etcd.io/etcd v0.5.0-alpha.5.0.20200910180754-dd1b699fc489 // ae9734ed278b is the SHA for git tag v3.4.13

replace google.golang.org/grpc => google.golang.org/grpc v1.27.1
