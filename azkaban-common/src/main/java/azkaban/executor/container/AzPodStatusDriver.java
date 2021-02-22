package azkaban.executor.container;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzPodStatusDriver implements RawPodWatchEventListener {
  private static final Logger logger = LoggerFactory.getLogger(AzPodStatusDriver.class);

  private static final int THREAD_POOL_SIZE = 4;
  private final ExecutorService executor;
  private final List<AzPodStatusListener> listeners = new ArrayList<>();
  private final ImmutableMap<AzPodStatus, List<Consumer<Response<V1Pod>>>> listenerMap;

  public AzPodStatusDriver() {
    executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE,
        new ThreadFactoryBuilder().setNameFormat("azk-watch-pool-%d").build());

    ImmutableMap.Builder listenerMapBuilder = ImmutableMap.builder()
        .put(AzPodStatus.AZ_POD_REQUESTED, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_SCHEDULED, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_READY, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_COMPLETED, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_INIT_FAILURE, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_APP_FAILURE, new ArrayList<>())
        .put(AzPodStatus.AZ_POD_UNEXPECTED, new ArrayList<>());
    listenerMap = listenerMapBuilder.build();
  }

  private void notifyListeners(AzPodStatusWithEvent statusWithEvent) {
    //todo: replace null with correct Watch.Response<>
    listenerMap.get(statusWithEvent.getAzPodStatus()).stream()
        .forEach(consumer -> consumer.accept(statusWithEvent.getPodWatchEvent()));
//    if (status == AzPodStatus.AZ_POD_REQUESTED) {
//      listeners.stream().forEach(listener -> listener.OnPodRequested(null));
//      return;
//    }
//    if (status == AzPodStatus.AZ_POD_SCHEDULED) {
//      listeners.stream().forEach(listener -> executor.submit(() -> listener.OnPodScheduled(null)));
//      return;
//    }
//    if (status == AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING) {
//      listeners.stream().forEach(listener -> listener.OnPodInitContainersRunning(null));
//      return;
//    }
//    if (status == AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING) {
//      listeners.stream().forEach(listener -> listener.OnPodAppContainersStarting(null));
//      return;
//    }
//    if (status == AzPodStatus.AZ_POD_READY) {
//      listeners.stream().forEach(listener -> listener.OnPodReady(null));
//      return;
//    }
//    if (status == AzPodStatus.AZ_POD_COMPLETED) {
//      listeners.stream().forEach(listener -> listener.OnPodCompleted(null));
//      return;
//    }
//    if (status == AzPodStatus.AZ_POD_INIT_FAILURE) {
//      listeners.stream().forEach(listener -> listener.OnPodInitFailure(null));
//      return;
//    }
//    if (status == AzPodStatus.AZ_POD_APP_FAILURE) {
//      listeners.stream().forEach(listener -> listener.OnPodAppFailure(null));
//      return;
//    }
//    if (status == AzPodStatus.AZ_POD_UNEXPECTED) {
//      listeners.stream().forEach(listener -> listener.OnPodUnexpected(null));
//      return;
//    }
  }

  @Override
  public void onEvent(Watch.Response<V1Pod> watchEvent) {
    logPodWatchEvent(watchEvent);
    try {
      AzPodStatusWithEvent azPodStatusWithEvent = AzPodStatusExtractor.azPodStatusFromEvent(watchEvent);
      notifyListeners(azPodStatusWithEvent);
    } catch (Exception e) {
      logger.error("Unexepcted exception while processing pod watch event.", e);
    }
  }

  private static void logPodWatchEvent(Watch.Response<V1Pod> watchEvent) {
    //todo
  }

  private void registerAzPodStatusListener(AzPodStatusListener listener) {
    requireNonNull(listener, "listener must not be null");
//    listeners.add(listener);
    listenerMap.get(AzPodStatus.AZ_POD_REQUESTED).add(listener::OnPodRequested);
    listenerMap.get(AzPodStatus.AZ_POD_SCHEDULED).add(listener::OnPodScheduled);
    listenerMap.get(AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING).add(listener::OnPodInitContainersRunning);
    listenerMap.get(AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING).add(listener::OnPodAppContainersStarting);
    listenerMap.get(AzPodStatus.AZ_POD_READY).add(listener::OnPodReady);
    listenerMap.get(AzPodStatus.AZ_POD_COMPLETED).add(listener::OnPodCompleted);
    listenerMap.get(AzPodStatus.AZ_POD_INIT_FAILURE).add(listener::OnPodInitFailure);
    listenerMap.get(AzPodStatus.AZ_POD_APP_FAILURE).add(listener::OnPodAppFailure);
    listenerMap.get(AzPodStatus.AZ_POD_UNEXPECTED).add(listener::OnPodUnexpected);

  }
}
