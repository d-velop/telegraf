package kube_state

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influxdata/telegraf/testutil"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPersistentVolumeClaim(t *testing.T) {
	cli := &client{
		httpClient: &http.Client{Transport: &http.Transport{}},
		semaphore:  make(chan struct{}, 1),
	}
	now := time.Now()
	now = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 1, 36, 0, now.Location())

	tests := []struct {
		name     string
		handler  *mockHandler
		output   *testutil.Accumulator
		hasError bool
	}{
		{
			name: "no pv claims",
			handler: &mockHandler{
				responseMap: map[string]interface{}{
					"/persistentvolumeclaims/": &v1.ServiceStatus{},
				},
			},
			hasError: false,
		},
		{
			name: "collect pv claims",
			handler: &mockHandler{
				responseMap: map[string]interface{}{
					"/persistentvolumeclaims/": &v1.PersistentVolumeClaimList{
						Items: []v1.PersistentVolumeClaim{
							{
								Status: v1.PersistentVolumeClaimStatus{
									Phase: v1.ClaimBound,
								},
								Spec: v1.PersistentVolumeClaimSpec{
									VolumeName:       "pvc-dc870fd6-1e08-11e8-b226-02aa4bc06eb8",
									StorageClassName: strPtr("ebs-1"),
								},
								ObjectMeta: metav1.ObjectMeta{
									Namespace: "ns1",
									Name:      "pc1",
									Labels: map[string]string{
										"lab1": "v1",
										"lab2": "v2",
									},
									CreationTimestamp: metav1.Time{Time: now},
								},
							},
						},
					},
				},
			},
			output: &testutil.Accumulator{
				Metrics: []*testutil.Metric{
					{
						Fields: map[string]interface{}{
							"status_lost":    0,
							"status_pending": 0,
							"status_bound":   1,
						},
						Tags: map[string]string{
							"label_lab1":            "v1",
							"label_lab2":            "v2",
							"namespace":             "ns1",
							"persistentvolumeclaim": "pc1",
							"storageclass":          "ebs-1",
							"volumename":            "pvc-dc870fd6-1e08-11e8-b226-02aa4bc06eb8",
							"status":                "bound",
						},
					},
				},
			},
			hasError: false,
		},
	}
	for _, v := range tests {
		ts := httptest.NewServer(v.handler)
		defer ts.Close()

		cli.baseURL = ts.URL
		ks := &KubenetesState{
			client: cli,
		}
		acc := new(testutil.Accumulator)
		registerPersistentVolumeClaimCollector(context.Background(), acc, ks)
		err := acc.FirstError()
		if err == nil && v.hasError {
			t.Fatalf("%s failed, should have error", v.name)
		} else if err != nil && !v.hasError {
			t.Fatalf("%s failed, err: %v", v.name, err)
		}
		if v.output == nil && len(acc.Metrics) > 0 {
			t.Fatalf("%s: collected extra data", v.name)
		} else if v.output != nil && len(v.output.Metrics) > 0 {
			for i := range v.output.Metrics {
				for k, m := range v.output.Metrics[i].Tags {
					if acc.Metrics[i].Tags[k] != m {
						t.Fatalf("%s: tag %s metrics unmatch Expected %s, got %s\n", v.name, k, m, acc.Metrics[i].Tags[k])
					}
				}
				for k, m := range v.output.Metrics[i].Fields {
					if acc.Metrics[i].Fields[k] != m {
						t.Fatalf("%s: field %s metrics unmatch Expected %v(%T), got %v(%T)\n", v.name, k, m, m, acc.Metrics[i].Fields[k], acc.Metrics[i].Fields[k])
					}
				}
			}
		}

	}
}

func strPtr(s string) *string {
	return &s
}