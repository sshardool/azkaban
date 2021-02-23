package azkaban.executor.container;

import static java.util.Objects.requireNonNull;

import azkaban.executor.ExecutorManagerException;
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
  private Watch<V1Pod> podWatch;
  private RawPodWatchEventListener podWatchEventListener;

  @Inject
  public KubernetesWatch(KubeConfig kubeConfig,
      RawPodWatchEventListener podWatchEventListener,
      PodWatchParams podWatchParams)throws ExecutorManagerException {
    requireNonNull(kubeConfig);
    requireNonNull(podWatchEventListener);
    requireNonNull(podWatchParams);
    this.podWatchEventListener = podWatchEventListener;
    this.podWatchParams = podWatchParams;
    try {
      this.client = ClientBuilder.kubeconfig(kubeConfig).build();
    } catch (IOException e) {
      final ExecutorManagerException eme = new ExecutorManagerException("Unable to create client", e);
      logger.error("Exception reported. ", eme);
      throw eme;
    }
    // no timeout for request completion
    OkHttpClient httpClient =
        client.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build();
    client.setHttpClient(httpClient);
    this.coreV1Api = new CoreV1Api(this.client);
  }

  public KubernetesWatch(KubeConfig kubeConfig,
      RawPodWatchEventListener podWatchEventListener,
      String namespace,
      String labelSelector) throws ExecutorManagerException {
    this(kubeConfig, podWatchEventListener, new PodWatchParams(namespace, labelSelector));
  }

  public KubernetesWatch(KubeConfig kubeConfig,
      RawPodWatchEventListener podWatchEventListener) throws ExecutorManagerException {
    this(kubeConfig, podWatchEventListener, new PodWatchParams(null, null));
  }

  /**
   * Create the Pod watch and set it up for creating parsed representations of the JSON
   * responses received from the Kubernetes API server. Responses will be converted to type
   * {@code Watch.Response<V1Pod>}.
   * Creating the watch submits the request the API server but does not block beyond that.
   * @throws ExecutorManagerException
   */
  public void initializePodWatch() throws ExecutorManagerException {
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
    } catch (ApiException e) {
      throw new ExecutorManagerException("Exception while creating pod watch.", e);
    }
  }

  @VisibleForTesting
  void processPodWatchEvents(Iterable<Watch.Response<V1Pod>> events) {
    requireNonNull(events, "watch events must not be null");
    for (Watch.Response<V1Pod> item : events) {
      this.podWatchEventListener.onEvent(item);
    }
  }

  /**
   * This starts the continuous event processing loop for fetching the pod watch events.
   * Processing of the events is callback driven and the registered {@code RawPodWatchEventListener}
   * provides the processing implementation.
   * @throws IOException
   */
  public void startPodWatch() throws IOException {
    requireNonNull(podWatch, "watch must be initialized");
    try {
      processPodWatchEvents(podWatch);
    } finally {
      //todo: reinitialize watch in case of failures.
      podWatch.close();
    }
  }

  /**
   * Parameters used for setting up the Pod watch.
   */
  public static class PodWatchParams {
    private final String namespace;
    private final String labelSelector;

    public PodWatchParams(String namespace, String labelSelector) {
      this.namespace = namespace;
      this.labelSelector = labelSelector;
    }

    public String getNamespace() {
      return namespace;
    }

    public String getLabelSelector() {
      return labelSelector;
    }
  }
}
