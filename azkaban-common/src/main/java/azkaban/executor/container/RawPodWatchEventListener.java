package azkaban.executor.container;

import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;

public interface RawPodWatchEventListener {
  public void onEvent(Watch.Response<V1Pod> watchEvent);
}
