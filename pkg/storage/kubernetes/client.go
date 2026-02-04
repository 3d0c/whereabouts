package kubernetes

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	whereaboutsv1alpha1 "github.com/k8snetworkplumbingwg/whereabouts/pkg/api/whereabouts.cni.cncf.io/v1alpha1"
	wbclient "github.com/k8snetworkplumbingwg/whereabouts/pkg/generated/clientset/versioned"
	"github.com/k8snetworkplumbingwg/whereabouts/pkg/logging"
	"github.com/k8snetworkplumbingwg/whereabouts/pkg/storage"
	whereaboutstypes "github.com/k8snetworkplumbingwg/whereabouts/pkg/types"
)

const (
	KindVirtualMachineInstance = "VirtualMachineInstance"
	// KindVirtualMachineInstanceMigration = "VirtualMachineInstanceMigration"

	LmigrationJobUID = "kubevirt.io/migrationJobUID"

	listRequestTimeout = 30 * time.Second
)

// Client has info on how to connect to the kubernetes cluster
type Client struct {
	client    wbclient.Interface
	clientSet kubernetes.Interface
	dynamic   dynamic.Interface
	retries   int
}

func NewClient() (*Client, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	return newClient(config)
}

func NewClientViaKubeconfig(kubeconfigPath string) (*Client, error) {
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath},
		&clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, err
	}

	return newClient(config)
}

func newClient(config *rest.Config) (*Client, error) {
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	c, err := wbclient.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	dyn, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return NewKubernetesClient(c, clientSet, dyn), nil
}

func NewKubernetesClient(k8sClient wbclient.Interface, k8sClientSet kubernetes.Interface, dyn dynamic.Interface) *Client {
	return &Client{
		client:    k8sClient,
		clientSet: k8sClientSet,
		dynamic:   dyn,
		retries:   storage.DatastoreRetries,
	}
}

func (i *Client) ListIPPools() ([]storage.IPPool, error) {
	logging.Debugf("listing IP pools")

	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), listRequestTimeout)
	defer cancel()

	ipPoolList, err := i.client.WhereaboutsV1alpha1().IPPools(metav1.NamespaceAll).List(ctxWithTimeout, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var whereaboutsApiIPPoolList []storage.IPPool
	for idx, pool := range ipPoolList.Items {
		firstIP, _, err := pool.ParseCIDR()
		if err != nil {
			return nil, err
		}
		whereaboutsApiIPPoolList = append(
			whereaboutsApiIPPoolList,
			&KubernetesIPPool{client: i.client, firstIP: firstIP, pool: &ipPoolList.Items[idx]})
	}
	return whereaboutsApiIPPoolList, nil
}

func (i *Client) ListPods() ([]v1.Pod, error) {
	logging.Debugf("listing Pods")

	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), listRequestTimeout)
	defer cancel()

	podList, err := i.clientSet.CoreV1().Pods(metav1.NamespaceAll).List(ctxWithTimeout, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return podList.Items, nil
}

type VMmap map[string]struct{}

func (m VMmap) Has(vmRef string) bool {
	_, ok := m[vmRef]
	return ok
}

func (i *Client) ListVMs() (VMmap, error) {
	vmMap := make(VMmap, 0)

	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), storage.RequestTimeout)
	defer cancel()

	vmGVR := schema.GroupVersionResource{
		Group:    "kubevirt.io",
		Version:  "v1",
		Resource: "virtualmachines",
	}

	vmList, err := i.dynamic.
		Resource(vmGVR).
		Namespace(metav1.NamespaceAll).
		List(ctxWithTimeout, metav1.ListOptions{})
	if err != nil {
		logging.Errorf("Error getting virtual machines list: %s, ignoring this cause persisten IP for VM is optional", err)
		return vmMap, nil
	}

	for _, vm := range vmList.Items {
		vmMap[vm.GetNamespace()+"/"+vm.GetName()] = struct{}{}
	}

	return vmMap, nil
}

func (i *Client) GetPod(namespace, name string) (*v1.Pod, error) {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), storage.RequestTimeout)
	defer cancel()

	pod, err := i.clientSet.CoreV1().Pods(namespace).Get(ctxWithTimeout, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return pod, nil
}

func (i *Client) GetVmim(namespace, migrationUID string) (map[string]interface{}, error) {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), storage.RequestTimeout)
	defer cancel()

	vmimGVR := schema.GroupVersionResource{
		Group:    "kubevirt.io",
		Version:  "v1",
		Resource: "virtualmachineinstancemigrations",
	}

	vmims, err := i.dynamic.
		Resource(vmimGVR).
		Namespace(namespace).
		List(ctxWithTimeout, metav1.ListOptions{})
	if err != nil {
		logging.Errorf("Error getting VMIMs list: %v", err)
		return nil, err
	}

	for _, vmim := range vmims.Items {
		if string(vmim.GetUID()) == migrationUID {
			return vmim.Object, nil
		}
	}

	return nil, fmt.Errorf("VMIM %s/%s not found", namespace, migrationUID)
}

func (i *Client) GetSourcePod(namespace, migrationUID string) (string, error) {
	vmim, err := i.GetVmim(namespace, migrationUID)
	if err != nil {
		logging.Errorf("Error getting VMIM: %s", err)
		return "", nil
	}

	sourcePod, found, err := unstructured.NestedString(
		vmim,
		"status",
		"migrationState",
		"sourcePod",
	)
	if err != nil {
		logging.Errorf("Error extracting sourcePod from VMIM %s/%s: %s", namespace, migrationUID, err)
		return "", err
	}
	if !found || sourcePod == "" {
		logging.Errorf("VMIM %s/%s has no sourcePod yet", namespace, migrationUID)
		return "", nil
	}

	return sourcePod, nil
}

// GetVMRef returns VirtualMachine reference if found.
// Errors are treated as warnings for now to not to break original flow.
// It also returns source pod if there is a migration process in progress.
// Happy pass for VM with persistent IP is: to have vmref or vmref and source pod.
// In all other ways whereabouts process left intact.
func (i *Client) GetVMRef(ipamConf whereaboutstypes.IPAMConfig) (string, string, error) {
	var (
		vmRef  string
		srcPod string
		err    error
	)

	p, err := i.GetPod(ipamConf.PodNamespace, ipamConf.PodName)
	if err != nil {
		logging.Errorf("Unable to get pod '%s': %v", ipamConf.GetPodRef(), err)
		return vmRef, srcPod, nil
	}

	labels := p.GetLabels()
	migrationUID, ok := labels[LmigrationJobUID]
	if ok {
		if srcPod, err = i.GetSourcePod(ipamConf.PodNamespace, migrationUID); err != nil {
			logging.Errorf("Unable to get Source Pod: %s", err)
			return "", "", nil
		}
	}

	or := p.GetOwnerReferences()
	for _, entry := range or {
		if entry.Kind == KindVirtualMachineInstance {
			vmRef = ipamConf.PodNamespace + "/" + entry.Name
			logging.Debugf("IPAM Management -- Kind: %s, vmRef: %s", entry.Kind, vmRef)
			break
		}
	}

	return vmRef, srcPod, nil
}

func (i *Client) ListOverlappingIPs() ([]whereaboutsv1alpha1.OverlappingRangeIPReservation, error) {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), listRequestTimeout)
	defer cancel()

	overlappingIPsList, err := i.client.WhereaboutsV1alpha1().OverlappingRangeIPReservations(metav1.NamespaceAll).List(ctxWithTimeout, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return overlappingIPsList.Items, nil
}

func (i *Client) DeleteOverlappingIP(clusterWideIP *whereaboutsv1alpha1.OverlappingRangeIPReservation) error {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), storage.RequestTimeout)
	defer cancel()

	return i.client.WhereaboutsV1alpha1().OverlappingRangeIPReservations(clusterWideIP.GetNamespace()).Delete(
		ctxWithTimeout, clusterWideIP.GetName(), metav1.DeleteOptions{})
}
