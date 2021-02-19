package controllers_test

import (
	"context"
	"fmt"
	"github.com/rabbitmq/cluster-operator/internal/status"
	"k8s.io/utils/pointer"
	runtimeClient "sigs.k8s.io/controller-runtime/pkg/client"

	rabbitmqv1beta1 "github.com/rabbitmq/cluster-operator/api/v1beta1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("Reconcile Scale Down", func() {
	var (
		cluster          *rabbitmqv1beta1.RabbitmqCluster
		defaultNamespace = "default"
		ctx              = context.Background()
	)
	AfterEach(func() {
		Expect(client.Delete(ctx, cluster)).To(Succeed())
		Eventually(func() bool {
			rmq := &rabbitmqv1beta1.RabbitmqCluster{}
			err := client.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, rmq)
			return apierrors.IsNotFound(err)
		}, 5).Should(BeTrue())
	})

	When("AllowScaleDown is not set", func() { // defaults to false
		It("logs errors, set ReconcileSucccess to false, and publish 'Warning' event", func() {
			cluster = &rabbitmqv1beta1.RabbitmqCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rabbitmq-shrink",
					Namespace: defaultNamespace,
				},
				Spec: rabbitmqv1beta1.RabbitmqClusterSpec{
					Replicas: pointer.Int32Ptr(5),
				},
			}
			Expect(client.Create(ctx, cluster)).To(Succeed())
			waitForClusterCreation(ctx, cluster, client)

			Expect(updateWithRetry(cluster, func(r *rabbitmqv1beta1.RabbitmqCluster) {
				r.Spec.Replicas = pointer.Int32Ptr(3)
			})).To(Succeed())

			// verify Status.Condition 'ReconcileSuccess' is set to false
			// with specific reason and message
			Eventually(func() string {
				rabbit := &rabbitmqv1beta1.RabbitmqCluster{}
				Expect(client.Get(ctx, runtimeClient.ObjectKey{
					Name:      cluster.Name,
					Namespace: defaultNamespace,
				}, rabbit)).To(Succeed())

				for i := range rabbit.Status.Conditions {
					if rabbit.Status.Conditions[i].Type == status.ReconcileSuccess {
						return fmt.Sprintf(
							"ReconcileSuccess status: %s, with reason: %s and message: %s",
							rabbit.Status.Conditions[i].Status,
							rabbit.Status.Conditions[i].Reason,
							rabbit.Status.Conditions[i].Message)
					}
				}
				return "ReconcileSuccess status: condition not present"
			}, 5).Should(Equal("ReconcileSuccess status: False, " +
				"with reason: FailedScaleDown " +
				"and message: [ScaleDown] 'AllowScaleDown' is set to false; abort request to scale down cluster"))

			// verify that FailedScaleDown event is recorded
			Expect(aggregateEventMsgs(ctx, cluster, "FailedScaleDown")).To(
				ContainSubstring("[ScaleDown] 'AllowScaleDown' is set to false; abort request to scale down cluster"))
		})
	})
})
