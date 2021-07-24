package grpcplugin

import (
  "context"
  "log"
  "time"

  v1 "k8s.io/api/core/v1"
  "k8s.io/autoscaler/cluster-autoscaler/expander"
  "k8s.io/autoscaler/cluster-autoscaler/expander/random"
  "k8s.io/klog/v2"
  schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"

  "google.golang.org/grpc"
  "google.golang.org/grpc/credentials"
)

type grpcclientstrategy struct {
  fallbackStrategy expander.Strategy
  grpcClient ExpanderClient
}

// NewFilter returns an expansion filter that
// TODO fix this filter remove duplicate code after working. - change bestOption proto to bestOptions to dedupe
func NewFilter(grpcExpanderCert string, grpcExpanderURL string) expander.Filter {
  var dialOpt grpc.DialOption
  // if no Cert file specified, use insecure
  if grpcExpanderCert == "" {
    dialOpt = grpc.WithInsecure()
  } else {
    creds, err := credentials.NewClientTLSFromFile(grpcExpanderCert, "")
    if err != nil {
      log.Fatalf("Failed to create TLS credentials %v", err)
    }
    dialOpt = grpc.WithTransportCredentials(creds)
  }

  klog.V(1).Info("Dialing ", grpcExpanderURL, " dialopt: ", dialOpt)
  conn, err := grpc.Dial(grpcExpanderURL, dialOpt)
  if err != nil {
    log.Fatalf("fail to dial server: %v", err)
  }

  client := NewExpanderClient(conn)
  return &grpcclientstrategy{fallbackStrategy: random.NewStrategy(), grpcClient: client}
}

func (g *grpcclientstrategy) BestOptions(expansionOptions []expander.Option, nodeInfo map[string]*schedulerframework.NodeInfo) []expander.Option {
  // Transform inputs to gRPC inputs
  nodeGroupIDOptionMap := make(map[string]expander.Option)
  grpcOptionsSlice := []*Option{}
  populateOptionsForGRPC(expansionOptions, nodeGroupIDOptionMap, &grpcOptionsSlice)
  grpcNodeInfoMap := make(map[string]*v1.Node)
  populateNodeInfoForGRPC(nodeInfo, grpcNodeInfoMap)

  // call gRPC server to get BestOption
  klog.V(1).Info("GPRC call of best options to server with ", len(nodeGroupIDOptionMap), " options")
  ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
  defer cancel()
  bestOptionsResponse, err := g.grpcClient.BestOptions(ctx, &BestOptionsRequest{Options: grpcOptionsSlice, NodeInfoMap: grpcNodeInfoMap})
  // TODO: remove fallback strat
  if err != nil {
    klog.V(1).Info("GRPC call timed out, no options filtered")
    return expansionOptions
  }

  if bestOptionsResponse.Options == nil {
    klog.V(1).Info("GRPC returned nil bestOptions, no options filtered")
    return expansionOptions
  }
  // Transform back options slice
  options := transformAndSanitizeOptionsFromGRPC(bestOptionsResponse.Options, nodeGroupIDOptionMap)
  if options == nil {
    klog.V(1).Info("Unable to sanitize GPRC returned bestOptions, no options filtered")
    return expansionOptions
  }
  return options
}

// populateOptionsForGRPC creates a map of nodegroup ID and options, as well as a slice of Options objects for the gRPC call
func populateOptionsForGRPC(expansionOptions []expander.Option, nodeGroupIDOptionMap map[string]expander.Option, grpcOptionsSlice *[]*Option) {
  for _, option := range expansionOptions {
    nodeGroupIDOptionMap[option.NodeGroup.Id()] = option
    *grpcOptionsSlice = append(*grpcOptionsSlice, newOptionMessage(option.NodeGroup.Id(), int32(option.NodeCount), option.Debug, option.Pods))
  }
}

// populateNodeInfoForGRPC modifies the nodeInfo object, and replaces it with the v1.Node to pass through grpc
func populateNodeInfoForGRPC(nodeInfos map[string]*schedulerframework.NodeInfo, grpcNodeInfoMap map[string]*v1.Node) {
  for nodeId, nodeInfo := range nodeInfos {
    grpcNodeInfoMap[nodeId] = nodeInfo.Node()
  }
}

func transformAndSanitizeOptionsFromGRPC(bestOptionsResponseOptions []*Option, nodeGroupIDOptionMap map[string]expander.Option) ([]expander.Option) {
  var options []expander.Option
  for _, option := range bestOptionsResponseOptions {
    if _, ok := nodeGroupIDOptionMap[option.NodeGroupId]; ok {
      options = append(options, nodeGroupIDOptionMap[option.NodeGroupId])
    } else {
      klog.Errorf("gRPC server returned invalid nodeGroup ID: ", option.NodeGroupId)
      return nil
    }

  }
  return options
}

func newOptionMessage(nodeGroupId string, nodeCount int32, debug string, pods []*v1.Pod) *Option{
  return &Option{NodeGroupId: nodeGroupId, NodeCount: nodeCount, Debug: debug, Pod: pods}
}
