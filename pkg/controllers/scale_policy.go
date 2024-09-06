package controllers

import (
	"context"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

func (r *JobSetReconciler) reconcileHPA(ctx context.Context, js *jobset.JobSet) error {

	log := ctrl.LoggerFrom(ctx)

	if js.Spec.ScalePolicy == nil || js.Spec.ScalePolicy.Metrics == nil {
		log.V(1).Info(
			"No ScalePolicy or Metric is specified, skipping HPA reconciling process")
		return nil
	}

	current := &autoscalingv2.HorizontalPodAutoscaler{}

	// Get the expected HPA.
	expected, err := desiredHPA(js, r.Scheme)
	if err != nil {
		return err
	}

	err = r.Get(ctx, client.ObjectKeyFromObject(expected), current)
	if err != nil {
		if errors.IsNotFound(err) {
			if jobSetSuspended(js) {
				// If the job is suspended, it's correct behavior that HPA doesn't exist.
				return nil
			}
			// Create the new HPA.
			log.V(1).Info("Creating HPA", "namespace", expected.Namespace, "name", expected.Name)
			return r.Create(ctx, expected)
		}
		return err
	}
	if jobSetSuspended(js) {
		// Delete the current HPA
		log.V(1).Info("Deleting HPA", "HorizontalPodAutoscaler", klog.KObj(current))
		return r.Delete(ctx, current)
	}

	if !equality.Semantic.DeepEqual(expected.Spec, current.Spec) {
		log.V(1).Info("Updating HPA", "namespace", current.Namespace, "name", current.Name)
		expected.ResourceVersion = current.ResourceVersion
		err = r.Update(context.TODO(), expected)
		if err != nil {
			return err
		}
	}
	return nil
}

func desiredHPA(js *jobset.JobSet, scheme *runtime.Scheme) (
	*autoscalingv2.HorizontalPodAutoscaler, error) {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      js.Name,
			Namespace: js.Namespace,
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				Kind:       js.Kind,
				Name:       js.Name,
				APIVersion: js.APIVersion,
			},
			MinReplicas: js.Spec.ScalePolicy.MinReplicas,
			MaxReplicas: *js.Spec.ScalePolicy.MaxReplicas,
			Metrics:     js.Spec.ScalePolicy.Metrics,
		},
	}
	if err := controllerruntime.SetControllerReference(js, hpa, scheme); err != nil {
		return nil, err
	}
	return hpa, nil
}
