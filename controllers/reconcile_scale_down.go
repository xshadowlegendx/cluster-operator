package controllers

import (
	"context"
	"errors"
	"fmt"
	rabbitmqv1beta1 "github.com/rabbitmq/cluster-operator/api/v1beta1"
	"github.com/rabbitmq/cluster-operator/internal/resource"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

func (r *RabbitmqClusterReconciler) reconcileScaleDown(ctx context.Context, builder resource.ResourceBuilder, cluster *rabbitmqv1beta1.RabbitmqCluster, resource client.Object) error {
	logger := ctrl.LoggerFrom(ctx)

	sts := resource.(*appsv1.StatefulSet)
	current, err := r.statefulSet(ctx, cluster)
	if client.IgnoreNotFound(err) != nil {
		return err
	} else if k8serrors.IsNotFound(err) {
		logger.Info("statefulSet not created yet, skipping checks to scale down the cluster")
		return nil
	}

	if err := builder.Update(sts); err != nil {
		return err
	}

	currentReplicas := *current.Spec.Replicas
	desiredReplicas := *sts.Spec.Replicas

	if r.needsScaleDown(currentReplicas, desiredReplicas) {
		if err := r.scaleDown(ctx, cluster, currentReplicas, desiredReplicas); err != nil {
			return err
		}
	}
	return nil
}

func (r *RabbitmqClusterReconciler) needsScaleDown(current, desired int32) bool {
	if current > desired {
		return true
	}
	return false
}

func (r *RabbitmqClusterReconciler) scaleDown(ctx context.Context, rmq *rabbitmqv1beta1.RabbitmqCluster, current, desired int32) error {
	logger := ctrl.LoggerFrom(ctx)

	if !rmq.Spec.AllowScaleDown {
		msg := "[ScaleDown] 'AllowScaleDown' is set to false; abort request to scale down cluster"
		err := errors.New(msg)
		logger.Error(err, "failed to scale down cluster")
		r.Recorder.Event(rmq, corev1.EventTypeWarning, "FailedScaleDown", msg)
		return err
	}

	logger.Info(fmt.Sprintf("[ScaleDown] cluster scale down from %d to %d", current, desired))

	if err := r.deleteSts(ctx, rmq); err != nil {
		return err
	}

	if err := r.deletePods(ctx, rmq, current, desired); err != nil {
		return err
	}

	r.Recorder.Event(rmq, corev1.EventTypeNormal, "SuccessfulScaleDown", fmt.Sprintf("Successful scale down from %d to %d node", current, desired))
	return nil
}

func (r *RabbitmqClusterReconciler) deletePods(ctx context.Context, rmq *rabbitmqv1beta1.RabbitmqCluster, currentReplicas, desiredReplicas int32) error {
	logger := ctrl.LoggerFrom(ctx)

	for i := currentReplicas; i > desiredReplicas; i-- {
		podName := rmq.PodName(i - 1)
		logger.Info("[ScaleDown] deleting pod", "pod", podName)
		pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: rmq.Namespace, Name: podName}}
		if err := r.Delete(ctx, &pod, &client.DeleteOptions{}); err != nil {
			msg := "[ScaleDown] failed to delete pod"
			logger.Error(err, msg, "pod", podName)
			r.Recorder.Event(rmq, corev1.EventTypeWarning, "FailedScaleDown", fmt.Sprintf("%s %s", msg, podName))
			return fmt.Errorf("%s %s: %v", msg, podName, err)
		}

		retries := int(*rmq.Spec.TerminationGracePeriodSeconds) / 5
		if err := retryWithInterval(logger, "delete pod", retries, 5*time.Second, func() bool {
			getErr := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: rmq.Namespace}, &pod)
			if k8serrors.IsNotFound(getErr) {
				return true
			}
			return false
		}); err != nil {
			msg := "[ScaleDown] failed to delete pod after 500 seconds"
			logger.Error(err, msg, "pod", podName)
			r.Recorder.Event(rmq, corev1.EventTypeWarning, "FailedScaleDown", fmt.Sprintf("%s %s", msg, podName))
			return fmt.Errorf("%s %s: %v", msg, podName, err)
		}
		logger.Info("[ScaleDown] successfully deleted pod for scaling down", "pod", podName)

		if err := r.shrinkNode(ctx, rmq, rmq.NodeName(i-1)); err != nil {
			return err
		}

		if err := r.forgetNode(ctx, rmq, rmq.NodeName(i-1)); err != nil {
			return err
		}
	}
	return nil
}

func (r *RabbitmqClusterReconciler) shrinkNode(ctx context.Context, rmq *rabbitmqv1beta1.RabbitmqCluster, nodeName string) error {
	logger := ctrl.LoggerFrom(ctx)

	podName := rmq.PodName(0)
	logger.Info("[ScaleDown] shrink node after deletion", "node", nodeName)
	cmd := fmt.Sprintf("rabbitmq-queues shrink %s", nodeName)
	stdout, stderr, err := r.exec(rmq.Namespace, podName, "rabbitmq", "bash", "-c", cmd)
	if err != nil {
		msg := "failed to shrink node"
		logger.Error(err, msg, "pod", podName, "node", nodeName, "command", cmd, "stdout", stdout, "stderr", stderr)
		r.Recorder.Event(rmq, corev1.EventTypeWarning, "FailedScaleDown", fmt.Sprintf("%s %s", msg, nodeName))
		return fmt.Errorf("%s %s: %v", msg, nodeName, err)
	}

	logger.Info("[ScaleDown] successfully shrink node after deletion", "node", nodeName)
	return nil
}

func (r *RabbitmqClusterReconciler) forgetNode(ctx context.Context, rmq *rabbitmqv1beta1.RabbitmqCluster, nodeName string) error {
	logger := ctrl.LoggerFrom(ctx)

	podName := rmq.PodName(0)
	logger.Info("[ScaleDown] forget node after deletion", "node", nodeName)
	cmd := fmt.Sprintf("rabbitmqctl forget_cluster_node %s", nodeName)
	stdout, stderr, err := r.exec(rmq.Namespace, podName, "rabbitmq", "bash", "-c", cmd)
	if err != nil {
		msg := "failed to forget node"
		logger.Error(err, msg, "pod", podName, "node", nodeName, "command", cmd, "stdout", stdout, "stderr", stderr)
		r.Recorder.Event(rmq, corev1.EventTypeWarning, "FailedScaleDown", fmt.Sprintf("%s %s", msg, nodeName))
		return fmt.Errorf("%s %s: %v", msg, nodeName, err)
	}

	logger.Info("[ScaleDown] successfully forget node after deletion", "node", nodeName)
	return nil
}
