package pomerium

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/pomerium/ingress-controller/model"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
)

type cacheTestDataBrokerClient struct {
	databroker.DataBrokerServiceClient
	record *databroker.Record
	get    int
	put    int
	putErr error
}

func (c *cacheTestDataBrokerClient) Get(
	_ context.Context,
	_ *databroker.GetRequest,
	_ ...grpc.CallOption,
) (*databroker.GetResponse, error) {
	c.get++
	if c.record == nil {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return &databroker.GetResponse{Record: proto.Clone(c.record).(*databroker.Record)}, nil
}

func (c *cacheTestDataBrokerClient) Put(
	_ context.Context,
	req *databroker.PutRequest,
	_ ...grpc.CallOption,
) (*databroker.PutResponse, error) {
	c.put++
	if c.putErr != nil {
		return nil, c.putErr
	}
	c.record = proto.Clone(req.Records[0]).(*databroker.Record)
	return &databroker.PutResponse{Records: []*databroker.Record{c.record}}, nil
}

func cacheTestIngressConfig() *model.IngressConfig {
	ic := validIngressConfig("app", "app.localhost.pomerium.io")
	ic.Ingress.UID = "ingress-uid"
	ic.Ingress.Generation = 7
	ic.Ingress.ResourceVersion = "100"
	ic.Ingress.Annotations = map[string]string{"ingress.pomerium.io/pass_identity_headers": "true"}
	ic.Ingress.Labels = map[string]string{"app": "test"}
	ic.Ingress.Status = networkingv1.IngressStatus{
		LoadBalancer: networkingv1.IngressLoadBalancerStatus{
			Ingress: []networkingv1.IngressLoadBalancerIngress{{IP: "192.0.2.1"}},
		},
	}

	name := types.NamespacedName{Name: "service", Namespace: "default"}
	ic.Services[name].UID = "service-uid"
	ic.Services[name].ResourceVersion = "200"
	ic.Endpoints = map[types.NamespacedName]*corev1.Endpoints{
		name: {
			ObjectMeta: metav1.ObjectMeta{
				Name:            name.Name,
				Namespace:       name.Namespace,
				UID:             "endpoints-uid",
				ResourceVersion: "300",
			},
			Subsets: []corev1.EndpointSubset{{
				Addresses: []corev1.EndpointAddress{{IP: "10.0.0.1"}},
			}},
		},
	}
	secretName := types.NamespacedName{Name: "tls", Namespace: "default"}
	ic.Secrets = map[types.NamespacedName]*corev1.Secret{
		secretName: {
			ObjectMeta: metav1.ObjectMeta{
				Name:            secretName.Name,
				Namespace:       secretName.Namespace,
				UID:             "secret-uid",
				ResourceVersion: "400",
			},
			Type: corev1.SecretTypeTLS,
			Data: map[string][]byte{corev1.TLSCertKey: []byte("certificate")},
		},
	}
	return ic
}

func TestIngressConfigCacheIgnoresStatusOnlyUpdates(t *testing.T) {
	before := cacheTestIngressConfig()
	after := cacheTestIngressConfig()
	after.Ingress.ResourceVersion = "101"
	after.Ingress.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "controller"}}
	after.Ingress.Status.LoadBalancer.Ingress[0].IP = "192.0.2.2"

	beforeFingerprint, err := fingerprintIngressConfig(before)
	require.NoError(t, err)
	afterFingerprint, err := fingerprintIngressConfig(after)
	require.NoError(t, err)
	assert.Equal(t, beforeFingerprint, afterFingerprint)
}

func TestIngressConfigCacheDetectsConfigInputs(t *testing.T) {
	base := cacheTestIngressConfig()
	baseFingerprint, err := fingerprintIngressConfig(base)
	require.NoError(t, err)

	tests := map[string]func(*model.IngressConfig){
		"annotation": func(ic *model.IngressConfig) {
			ic.Ingress.Annotations["ingress.pomerium.io/pass_identity_headers"] = "false"
		},
		"label": func(ic *model.IngressConfig) {
			ic.Ingress.Labels["acme.cert-manager.io/http01-solver"] = "true"
		},
		"spec": func(ic *model.IngressConfig) {
			ic.Ingress.Spec.Rules[0].Host = "changed.localhost.pomerium.io"
		},
		"service": func(ic *model.IngressConfig) {
			ic.Services[types.NamespacedName{Name: "service", Namespace: "default"}].Spec.ClusterIP = "10.96.0.10"
		},
		"endpoints": func(ic *model.IngressConfig) {
			ic.Endpoints[types.NamespacedName{Name: "service", Namespace: "default"}].Subsets[0].Addresses[0].IP = "10.0.0.2"
		},
		"secret": func(ic *model.IngressConfig) {
			ic.Secrets[types.NamespacedName{Name: "tls", Namespace: "default"}].Data[corev1.TLSCertKey] = []byte("changed")
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			ic := cacheTestIngressConfig()
			mutate(ic)
			fingerprint, err := fingerprintIngressConfig(ic)
			require.NoError(t, err)
			assert.NotEqual(t, baseFingerprint, fingerprint)
		})
	}
}

func TestIngressConfigCacheShortCircuitsDuplicateUpsert(t *testing.T) {
	ic := cacheTestIngressConfig()
	fingerprint, err := fingerprintIngressConfig(ic)
	require.NoError(t, err)

	r := &DataBrokerReconciler{}
	r.ingressConfigCache.store(ic.GetIngressNamespacedName(), fingerprint)

	// A cache miss would try the nil DataBroker client and panic. A duplicate
	// returns before cloning, validating, or contacting DataBroker.
	changed, err := r.Upsert(context.Background(), ic)
	require.NoError(t, err)
	assert.False(t, changed)
}

func TestIngressConfigCacheSetPrimesUpsert(t *testing.T) {
	ctx := context.Background()
	client := new(cacheTestDataBrokerClient)
	r := &DataBrokerReconciler{
		ConfigID:                IngressControllerConfigID,
		DataBrokerServiceClient: client,
	}

	ic := cacheTestIngressConfig()
	changed, err := r.Set(ctx, []*model.IngressConfig{ic})
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, 1, client.get)
	assert.Equal(t, 1, client.put)

	duplicate := cacheTestIngressConfig()
	duplicate.Ingress.ResourceVersion = "101"
	duplicate.Ingress.Status.LoadBalancer.Ingress[0].IP = "192.0.2.2"
	changed, err = r.Upsert(ctx, duplicate)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Equal(t, 1, client.get, "duplicate Upsert should not read DataBroker")
	assert.Equal(t, 1, client.put, "duplicate Upsert should not write DataBroker")

	duplicate.Ingress.Spec.Rules[0].Host = "changed.localhost.pomerium.io"
	changed, err = r.Upsert(ctx, duplicate)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, 2, client.get, "a config change should read DataBroker")
	assert.Equal(t, 2, client.put, "a config change should write DataBroker")
}

func TestIngressConfigCacheDoesNotStoreFailedSet(t *testing.T) {
	client := &cacheTestDataBrokerClient{putErr: errors.New("put failed")}
	r := &DataBrokerReconciler{
		ConfigID:                IngressControllerConfigID,
		DataBrokerServiceClient: client,
	}
	ic := cacheTestIngressConfig()

	_, err := r.Set(context.Background(), []*model.IngressConfig{ic})
	require.ErrorContains(t, err, "put failed")

	fingerprint, fingerprintErr := fingerprintIngressConfig(ic)
	require.NoError(t, fingerprintErr)
	assert.False(t, r.ingressConfigCache.hit(ic.GetIngressNamespacedName(), fingerprint))
}

func TestIngressConfigCacheReplaceAndDelete(t *testing.T) {
	ic := cacheTestIngressConfig()
	fingerprints, err := fingerprintIngressConfigs([]*model.IngressConfig{ic})
	require.NoError(t, err)

	var cache ingressConfigCache
	cache.replace(fingerprints)
	name := ic.GetIngressNamespacedName()
	fingerprint := fingerprints[name]
	assert.True(t, cache.hit(name, fingerprint))

	cache.delete(name)
	assert.False(t, cache.hit(name, fingerprint))
}

func BenchmarkIngressConfigCacheHit438(b *testing.B) {
	ics := make([]*model.IngressConfig, 438)
	for i := range ics {
		ic := cacheTestIngressConfig()
		ic.Ingress.Name = fmt.Sprintf("app-%03d", i)
		ic.Ingress.UID = types.UID(fmt.Sprintf("ingress-uid-%03d", i))
		ic.Ingress.Spec.Rules[0].Host = fmt.Sprintf("app-%03d.localhost.pomerium.io", i)
		ics[i] = ic
	}
	fingerprints, err := fingerprintIngressConfigs(ics)
	require.NoError(b, err)
	var cache ingressConfigCache
	cache.replace(fingerprints)

	b.ResetTimer()
	for range b.N {
		for _, ic := range ics {
			fingerprint, err := fingerprintIngressConfig(ic)
			if err != nil || !cache.hit(ic.GetIngressNamespacedName(), fingerprint) {
				b.Fatalf("cache miss: %v", err)
			}
		}
	}
}
