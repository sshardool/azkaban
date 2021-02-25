package azkaban.executor.container.watch;

import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;

public interface RawPodWatchEventListener {
  void onEvent(Watch.Response<V1Pod> watchEvent);
}
