package azkaban.executor.container;

import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;

public class AzPodStatusWithEvent {
  private final AzPodStatus azPodStatus;
  private final Watch.Response<V1Pod> podWatchEvent;

  public AzPodStatusWithEvent(AzPodStatus azPodStatus,
      Response<V1Pod> podWatchEvent) {
    this.azPodStatus = azPodStatus;
    this.podWatchEvent = podWatchEvent;
  }

  public AzPodStatus getAzPodStatus() {
    return azPodStatus;
  }

  public Response<V1Pod> getPodWatchEvent() {
    return podWatchEvent;
  }
}
