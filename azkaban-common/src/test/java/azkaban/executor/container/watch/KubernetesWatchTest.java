package azkaban.executor.container.watch;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.JSON;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesWatchTest {

  private static final Logger logger = LoggerFactory.getLogger(KubernetesWatchTest.class);
  public static final String NAMESPACE = "cop-dev";
  private static final List<AzPodStatus> podSuccessLifecycleStates = ImmutableList.of(
      AzPodStatus.AZ_POD_REQUESTED,
      AzPodStatus.AZ_POD_SCHEDULED,
      AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING,
      AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING,
      AzPodStatus.AZ_POD_READY,
      AzPodStatus.AZ_POD_COMPLETED);

  // todo: sanitize and package json files as resources
  private static String KUBE_CONFIG_PATH = "/Users/sshardoo/.kube/proxy-config";
  private static String JSON_EVENTS_FILE_PATH = "/Users/sshardoo/source/misc/sample5-pods.json";
  private static String POD_WITH_LIFECYCLE_SUCCESS = "fc-dep-holdem6-280";
  private static String POD_WITH_LIFECYCLE_INIT_FAILURE = "";
  private static String POD_WITH_LIFECYCLE_APP_FAILURE = "";
  private static String POD_WTIH_COMPLETED_STATE = "fc-dep-holdem6-238";

  private KubeConfig kubeConfig;
  private KubernetesWatch kubeWatchWithMockListener;

  @Before
  public void setUp() throws Exception {
    this.kubeConfig = KubeConfig.loadKubeConfig(
        Files.newBufferedReader(Paths.get(KUBE_CONFIG_PATH), Charset.defaultCharset()));
    this.kubeWatchWithMockListener = new KubernetesWatch(kubeConfig, new MockPodWatchEventListener(), NAMESPACE,
        null);
  }

  @Test
  public void testDriverWithStatusLoggingListener() throws Exception {
    StatusLoggingListener podStatusListener = new StatusLoggingListener();
    AzPodStatusDriver azPodStatusDriver = new AzPodStatusDriver();
    azPodStatusDriver.registerAzPodStatusListener(podStatusListener);
    KubernetesWatch watchWithLoggingListener = new KubernetesWatch(kubeConfig,
        azPodStatusDriver,
        NAMESPACE,
        null);
    ApiClient apiClient = ClientBuilder.kubeconfig(kubeConfig).build();
    FileBackedWatch<V1Pod> fileBackedWatch = new FileBackedWatch<>(
        apiClient.getJSON(),
        new TypeToken<Response<V1Pod>>() {}.getType(),
        Paths.get(JSON_EVENTS_FILE_PATH));
    watchWithLoggingListener.processPodWatchEvents(fileBackedWatch);
    azPodStatusDriver.shutdown();

    ConcurrentMap<String, Queue<AzPodStatus>> statusLogMap = podStatusListener.getStatusLogMap();
    StatusLoggingListener.logDebugStatusMap(statusLogMap);

    List<AzPodStatus> actualLifecycleStates =
        statusLogMap.get(POD_WITH_LIFECYCLE_SUCCESS).stream()
        .distinct().collect(Collectors.toList());

    Assert.assertEquals(podSuccessLifecycleStates, actualLifecycleStates);
  }

  @Test
  public void testWithMockedEvents() throws Exception {
    ApiClient apiClient = ClientBuilder.kubeconfig(kubeConfig).build();
    FileBackedWatch<V1Pod> fileBackedWatch = new FileBackedWatch<>(
        apiClient.getJSON(),
        new TypeToken<Response<V1Pod>>() {}.getType(),
        Paths.get(JSON_EVENTS_FILE_PATH));
    kubeWatchWithMockListener.processPodWatchEvents(fileBackedWatch);
  }

  @Test
  public void testPodWatch() throws Exception {
    kubeWatchWithMockListener.initializePodWatch();
    kubeWatchWithMockListener.startPodWatch();
  }

  private static class FileBackedWatch<T> extends Watch<T> {
    private final BufferedReader reader;

    public FileBackedWatch(JSON json, Type watchType, Path jsonEventsFile) throws IOException {
      super(json, null, watchType, null);
      requireNonNull(jsonEventsFile);
      reader = Files.newBufferedReader(jsonEventsFile, StandardCharsets.UTF_8);
    }

    @Override
    public Response<T> next() {
      try {
        String line = reader.readLine();
        if (line == null) {
          throw new RuntimeException("Line is null");
        }
        return parseLine(line);
      } catch (IOException e) {
        throw new RuntimeException("IO Exception during next method.", e);
      }
    }

    @Override
    public boolean hasNext() {
      try {
        return reader.ready();
      } catch (IOException e) {
        throw new RuntimeException("Exception in hasNext.", e);
      }
    }

    @Override
    public void close() throws IOException {
      if (reader != null) {
        reader.close();
      }
    }
  }

  private static class StatusLoggingListener implements AzPodStatusListener {
    private final ConcurrentMap<String, Queue<AzPodStatus>> statusLogMap =
        new ConcurrentHashMap<>();

    public static void logDebugStatusMap(ConcurrentMap<String, Queue<AzPodStatus>> statusLogMap) {
      statusLogMap.forEach((podname, queue) -> {
        StringBuilder sb = new StringBuilder(podname + ": ");
        queue.forEach(status -> sb.append(status.toString() + ", "));
        logger.debug(sb.toString());
      });
    }

    private void logStatusForPod(AzPodStatusMetadata event) {
      AzPodStatus podStatus = event.getAzPodStatus();
      String podName = event.getPodName();
      Queue<AzPodStatus> statusLog = statusLogMap.computeIfAbsent(
          podName, k -> new ConcurrentLinkedQueue<>());
      statusLog.add(podStatus);
    }

    public ConcurrentMap<String, Queue<AzPodStatus>> getStatusLogMap() {
      return statusLogMap;
    }

    @Override
    public void onPodRequested(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodScheduled(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodInitContainersRunning(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodAppContainersStarting(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodReady(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodCompleted(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodInitFailure(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodAppFailure(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodUnexpected(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }
  }

  private static class MockPodWatchEventListener implements RawPodWatchEventListener {
    @Override
    public void onEvent(Response<V1Pod> watchEvent) {
      logger.debug(String.format("%s : %s, %s, %s", watchEvent.type, watchEvent.object.getMetadata().getName(),
          watchEvent.object.getStatus().getMessage(), watchEvent.object.getStatus().getPhase()));
      AzPodStatus azPodStatus = AzPodStatusExtractor.azPodStatusFromEvent(watchEvent).getAzPodStatus();
      logger.debug("AZ_POD_STATUS: " + azPodStatus);
    }
  }
}