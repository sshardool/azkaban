package azkaban.executor.container;

import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;

public interface AzPodStatusListener {
  default void OnPodRequested(AzPodStatusMetadata event) {}
  default void OnPodScheduled(AzPodStatusMetadata event) {}
  default void OnPodInitContainersRunning(AzPodStatusMetadata event) {}
  default void OnPodAppContainersStarting(AzPodStatusMetadata event) {}
  default void OnPodReady(AzPodStatusMetadata event) {}
  default void OnPodCompleted(AzPodStatusMetadata event) {}
  default void OnPodInitFailure(AzPodStatusMetadata event) {}
  default void OnPodAppFailure(AzPodStatusMetadata event) {}
  default void OnPodUnexpected(AzPodStatusMetadata event) {}
}
