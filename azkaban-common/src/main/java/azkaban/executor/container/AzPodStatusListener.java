package azkaban.executor.container;

import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;

public interface AzPodStatusListener {
  default void OnPodRequested(Watch.Response<V1Pod> event) {}
  default void OnPodScheduled(Watch.Response<V1Pod> event) {}
  default void OnPodInitContainersRunning(Watch.Response<V1Pod> event) {}
  default void OnPodAppContainersStarting(Watch.Response<V1Pod> event) {}
  default void OnPodReady(Watch.Response<V1Pod> event) {}
  default void OnPodCompleted(Watch.Response<V1Pod> event) {}
  default void OnPodInitFailure(Watch.Response<V1Pod> event) {}
  default void OnPodAppFailure(Watch.Response<V1Pod> event) {}
  default void OnPodUnexpected(Watch.Response<V1Pod> event) {}
}
