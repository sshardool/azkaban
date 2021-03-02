package azkaban.executor.container.watch;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.inject.Singleton;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides primitives for 'watching' change capture events of kubernetes resources.
 * This is currently aimed at the monitoring the state changes of FlowContainer pods, but can be
 * extended for including other Kubernetes resources.
 */
@Singleton
public class KubernetesWatch {
  private static final Logger logger = LoggerFactory.getLogger(KubernetesWatch.class);

  private final ApiClient client;
  private final CoreV1Api coreV1Api;
  private final PodWatchParams podWatchParams;
  private final Thread watchRunner;
  private Watch<V1Pod> podWatch;
  private RawPodWatchEventListener podWatchEventListener;
  private int podWatchInitCount = 0;
  private AtomicBoolean isShutdownRequested = new AtomicBoolean(false);

  @Inject
  public KubernetesWatch(KubeConfig kubeConfig,
      RawPodWatchEventListener podWatchEventListener,
      PodWatchParams podWatchParams) {
    requireNonNull(kubeConfig);
    requireNonNull(podWatchEventListener);
    requireNonNull(podWatchParams);
    this.podWatchEventListener = podWatchEventListener;
    this.podWatchParams = podWatchParams;
    try {
      this.client = ClientBuilder.kubeconfig(kubeConfig).build();
    } catch (IOException e) {
      final WatchException we = new WatchException("Unable to create client", e);
      logger.error("Exception reported. ", we);
      throw we;
    }
    // no timeout for request completion
    OkHttpClient httpClient =
        client.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build();
    client.setHttpClient(httpClient);
    this.coreV1Api = new CoreV1Api(this.client);

    watchRunner = new Thread(this::initializeAndStartPodWatch);
  }

//  public KubernetesWatch(KubeConfig kubeConfig,
//      RawPodWatchEventListener podWatchEventListener,
//      String namespace,
//      String labelSelector,
//      int resetDelayMillis) {
//    this(kubeConfig, podWatchEventListener, new PodWatchParams(namespace, labelSelector, resetDelayMillis));
//  }

  public KubernetesWatch(KubeConfig kubeConfig,
      RawPodWatchEventListener podWatchEventListener) {
    this(kubeConfig, podWatchEventListener, new PodWatchParams(null, null));
  }

  public int getPodWatchInitCount() {
    return podWatchInitCount;
  }

  @VisibleForTesting
  protected void setPodWatch(Watch<V1Pod> podWatch) {
    requireNonNull(podWatch, "pod watch must not be null");
    this.podWatch = podWatch;
  }

  /**
   * Create the Pod watch and set it up for creating parsed representations of the JSON
   * responses received from the Kubernetes API server. Responses will be converted to type
   * {@code Watch.Response<V1Pod>}.
   * Creating the watch submits the request the API server but does not block beyond that.
   *
   * @throws ApiException
   */
  protected void initializePodWatch() throws ApiException {
    try {
      this.podWatch = Watch.createWatch(this.client,
          coreV1Api.listNamespacedPodCall(podWatchParams.getNamespace(),
              "true",
              false,
              null,
              null,
              podWatchParams.getLabelSelector(),
              null,
              null,
              null,
              true,
              null),
          new TypeToken<Response<V1Pod>>() {}.getType());
    } catch (ApiException ae) {
      logger.error("ApiException while creating pod watch.", ae);
      throw ae;
    }
  }

//  @VisibleForTesting
//  void processPodWatchEvents(Iterable<Watch.Response<V1Pod>> events) {
//    requireNonNull(events, "watch events must not be null");
//    for (Watch.Response<V1Pod> item : events) {
//      if (isShutdownRequested.get()) {
//        return;
//      }
//      this.podWatchEventListener.onEvent(item);
//    }
//  }

  /**
   * This starts the continuous event processing loop for fetching the pod watch events.
   * Processing of the events is callback driven and the registered {@link RawPodWatchEventListener}
   * provides the processing implementation.
   *
   * @throws IOException
   */
  protected void startPodWatch() throws IOException {
    requireNonNull(podWatch, "watch must be initialized");
    for (Watch.Response<V1Pod> item : podWatch) {
      if (isShutdownRequested.get()) {
        logger.info("Exiting pod watch event loop as shutdown was requested");
        return;
      }
      this.podWatchEventListener.onEvent(item);
    }
//    try {
//      processPodWatchEvents(podWatch);
//    } finally {
//      //todo: reinitialize watch in case of failures.
//      podWatch.close();
//    }
  }

  private void closePodWatchQuietly() {
    if (podWatch == null) {
      logger.debug("Pod watch is null");
      return;
    }
    try {
      podWatch.close();
    } catch (IOException e) {
      logger.error("IOException while closing pod watch.", e);
    }
  }

  public void initializeAndStartPodWatch() {
    while(!isShutdownRequested.get()) {
      try {
        podWatchInitCount++;
        logger.info("Initializing pod watch, initialization count: " + podWatchInitCount);
        initializePodWatch();
        startPodWatch();
      } catch (Exception e) {
        logger.warn("Exception during pod watch was suppressed.", e);
      } finally {
        logger.info("Closing pod watch");
        closePodWatchQuietly();
      }
      logger.info("Pod watch was terminated, will be reset with delay if shutdown was not "
          + "requested. Shutdown Requested is: " + isShutdownRequested.get());

      try {
        Thread.sleep(podWatchParams.getResetDelayMillis());
      } catch (InterruptedException e) {
        if (Thread.currentThread().isInterrupted()) {
          logger.info("Pod watch reset delay was interrupted.");
        }
      }
    }
  }

  public Thread launchPodWatch() {
    if (isShutdownRequested.get()) {
      logger.error("Pod watch was launched when shutdown already in progress");
      return null;
    }
    logger.info("Starting the pod watch thread");
    watchRunner.start();
    return watchRunner;
  }

  public boolean requestShutdown() {
    boolean notAlreadyRequested = isShutdownRequested.compareAndSet(false, true);
    if (!notAlreadyRequested) {
      logger.warn("Shutdown of kubernetes watch was already requested");
      return notAlreadyRequested;
    }
    logger.info("Requesting shutdown for kubernetes watch");
    watchRunner.interrupt();
    return notAlreadyRequested;
  }

  /**
   * Parameters used for setting up the Pod watch.
   */
  public static class PodWatchParams {
    private final static int DEFAULT_RESET_DELAY_MILLIS = 10 * 1000; //30 seconds
    private final String namespace;
    private final String labelSelector;
    private final int resetDelayMillis;

    public PodWatchParams(String namespace, String labelSelector, int resetDelayMillis) {
      this.namespace = namespace;
      this.labelSelector = labelSelector;
      this.resetDelayMillis = resetDelayMillis;
    }

    public PodWatchParams(String namespace, String labelSelector) {
      this(namespace, labelSelector, DEFAULT_RESET_DELAY_MILLIS);
    }

    public String getNamespace() {
      return namespace;
    }

    public String getLabelSelector() {
      return labelSelector;
    }

    public int getResetDelayMillis() {
      return resetDelayMillis;
    }
  }
}
