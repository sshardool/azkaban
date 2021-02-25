package azkaban.executor.container.watch;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class AzPodStatusDriver implements RawPodWatchEventListener {
  private static final Logger logger = LoggerFactory.getLogger(AzPodStatusDriver.class);

  private static final int THREAD_POOL_SIZE = 4;
  private final ExecutorService executor;
  private final ImmutableMap<AzPodStatus, List<Consumer<AzPodStatusMetadata>>> listenerMap;

  @Inject
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

  public void registerAzPodStatusListener(AzPodStatusListener listener) {
    requireNonNull(listener, "listener must not be null");
    listenerMap.get(AzPodStatus.AZ_POD_REQUESTED).add(listener::onPodRequested);
    listenerMap.get(AzPodStatus.AZ_POD_SCHEDULED).add(listener::onPodScheduled);
    listenerMap.get(AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING).add(listener::onPodInitContainersRunning);
    listenerMap.get(AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING).add(listener::onPodAppContainersStarting);
    listenerMap.get(AzPodStatus.AZ_POD_READY).add(listener::onPodReady);
    listenerMap.get(AzPodStatus.AZ_POD_COMPLETED).add(listener::onPodCompleted);
    listenerMap.get(AzPodStatus.AZ_POD_INIT_FAILURE).add(listener::onPodInitFailure);
    listenerMap.get(AzPodStatus.AZ_POD_APP_FAILURE).add(listener::onPodAppFailure);
    listenerMap.get(AzPodStatus.AZ_POD_UNEXPECTED).add(listener::onPodUnexpected);

  }

  public void shutdown() {
    executor.shutdown();
    try {
      executor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("Executor service shutdown was interrupted.", e);
    }
  }

  private void deliverCallbacksForEvent(AzPodStatusMetadata podStatusMetadata) {
    listenerMap.get(podStatusMetadata.getAzPodStatus()).stream()
        .forEach(callback -> executor.execute(() -> callback.accept(podStatusMetadata)));
  }

  @Override
  public void onEvent(Watch.Response<V1Pod> watchEvent) {
    // Technically, logging the pod watch event can also be performed as part of a callback.
    // For now the logging is inline to ensure the the event is logged before any corresponding
    // callbacks and are invoked.
    logPodWatchEvent(watchEvent);
    try {
      AzPodStatusMetadata azPodStatusMetadata = AzPodStatusExtractor.azPodStatusFromEvent(watchEvent);
      deliverCallbacksForEvent(azPodStatusMetadata);
    } catch (Exception e) {
      logger.error("Unexepcted exception while processing pod watch event.", e);
    }
  }

  private static void logPodWatchEvent(Watch.Response<V1Pod> watchEvent) {
    //todo
  }
}
