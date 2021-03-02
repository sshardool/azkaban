package azkaban.executor.container.watch;

import static java.util.Objects.requireNonNull;

import azkaban.executor.container.watch.KubernetesWatch.PodWatchParams;
import com.google.common.collect.ImmutableList;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.JSON;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Config;
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
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesWatchTest {

  private static final Logger logger = LoggerFactory.getLogger(KubernetesWatchTest.class);
  // todo: sanitize and package json files as resources
  private static String KUBE_CONFIG_PATH = "/Users/sshardoo/.kube/proxy-config";
  private static String JSON_EVENTS_FILE_PATH = "/Users/sshardoo/source/misc/sample5-pods.json";
  private final int DEFAULT_MAX_INIT_COUNT = 3;
  private final int DEFAULT_WATCH_RESET_DELAY_MILLIS = 100;
  private final int DEFAULT_WATCH_COMPLETION_TIMEOUT_MILLIS = 5000;
  public static final String DEFAULT_NAMESPACE = "cop-dev";
  private static String POD_WITH_LIFECYCLE_SUCCESS = "fc-dep-holdem6-280";
  private static final List<AzPodStatus> successfullFlowPodStateTransitionSequence = ImmutableList.of(
      AzPodStatus.AZ_POD_REQUESTED,
      AzPodStatus.AZ_POD_SCHEDULED,
      AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING,
      AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING,
      AzPodStatus.AZ_POD_READY,
      AzPodStatus.AZ_POD_COMPLETED);
  //  private static String POD_WITH_LIFECYCLE_INIT_FAILURE = "";
  //  private static String POD_WITH_LIFECYCLE_APP_FAILURE = "";
  //  private static String POD_WITH_COMPLETED_STATE = "";


  private KubeConfig localKubeConfig;

  @Before
  public void setUp() throws Exception {
    this.localKubeConfig = KubeConfig.loadKubeConfig(
        Files.newBufferedReader(Paths.get(KUBE_CONFIG_PATH), Charset.defaultCharset()));
  }

  private KubernetesWatch kubernetesWatchWithMockListener() {
    return new KubernetesWatch(localKubeConfig, new MockPodWatchEventListener(),
        new PodWatchParams(DEFAULT_NAMESPACE, null, DEFAULT_WATCH_RESET_DELAY_MILLIS));
  }

  private KubeConfig defaultKubeConfig() throws  IOException {
    return KubeConfig.loadKubeConfig(Files.newBufferedReader(Paths.get(KUBE_CONFIG_PATH), Charset.defaultCharset()));
  }

  private StatusLoggingListener statusLoggingListener() {
    return new StatusLoggingListener();
  }

  private AzPodStatusDriver statusDriverWithListener(AzPodStatusListener listener) {
    AzPodStatusDriver azPodStatusDriver = new AzPodStatusDriver();
    azPodStatusDriver.registerAzPodStatusListener(listener);
    return azPodStatusDriver;
  }


  private Watch<V1Pod> fileBackedWatch(ApiClient apiClient) throws  IOException {
    FileBackedWatch<V1Pod> fileBackedWatch = new FileBackedWatch<>(
        apiClient.getJSON(),
        new TypeToken<Response<V1Pod>>() {}.getType(),
        Paths.get(JSON_EVENTS_FILE_PATH));
    return fileBackedWatch;
  }

  private PreInitializedWatch defaultPreInitializedWatch(RawPodWatchEventListener driver,
      Watch<V1Pod> podWatch,
      int maxInitCount) throws IOException {
    return new PreInitializedWatch(defaultKubeConfig(),
        driver,
        podWatch,
        new PodWatchParams(null, null, DEFAULT_WATCH_RESET_DELAY_MILLIS),
        maxInitCount);
  }

  @Test
  public void testWatchShutdownAndResetAfterFailure() throws Exception {
    AzPodStatusDriver statusDriver = statusDriverWithListener(statusLoggingListener());
    Watch<V1Pod> fileBackedWatch = fileBackedWatch(Config.defaultClient());
    PreInitializedWatch kubernetesWatch = defaultPreInitializedWatch(statusDriver, fileBackedWatch,
        DEFAULT_MAX_INIT_COUNT);

    // FileBackedWatch with throw an IOException at read once the end of file has been reached.
    // PreInitWatch will auto-shutdown after the max_init_count resets.
    kubernetesWatch.launchPodWatch().join(DEFAULT_WATCH_COMPLETION_TIMEOUT_MILLIS);

    // Max watch init count should be less than or equal to the actual init count. Actual init
    // count can be larger by 1 as it may take upto 1 reset cycle for the shutdown request to be
    // evaluated.
    Assert.assertTrue(DEFAULT_MAX_INIT_COUNT <=  kubernetesWatch.getInitWatchCount());
    Assert.assertTrue(DEFAULT_MAX_INIT_COUNT <=  kubernetesWatch.getStartWatchCount());
    statusDriver.shutdown();
  }

  @Test
  public void testFlowPodCompleteTransition() throws Exception {
    StatusLoggingListener loggingListener = statusLoggingListener();
    AzPodStatusDriver statusDriver = statusDriverWithListener(loggingListener);
    Watch<V1Pod> fileBackedWatch = fileBackedWatch(Config.defaultClient());
    PreInitializedWatch kubernetesWatch = defaultPreInitializedWatch(statusDriver, fileBackedWatch,
        1);
    kubernetesWatch.launchPodWatch().join(DEFAULT_WATCH_COMPLETION_TIMEOUT_MILLIS);
    ConcurrentMap<String, Queue<AzPodStatus>> statusLogMap = loggingListener.getStatusLogMap();
    StatusLoggingListener.logDebugStatusMap(statusLogMap);

    List<AzPodStatus> actualLifecycleStates =
        statusLogMap.get(POD_WITH_LIFECYCLE_SUCCESS).stream()
        .distinct().collect(Collectors.toList());

    Assert.assertEquals(successfullFlowPodStateTransitionSequence, actualLifecycleStates);
    statusDriver.shutdown();
  }

  @Test
  @Ignore("Blocking watch execution, useful for development")
  public void testPodWatch() throws Exception {
    // Runs an unmodified instance of the azkaban watch that logs Raw watch events with debug
    // verbosity.
    KubernetesWatch kubernetesWatch = kubernetesWatchWithMockListener();
    kubernetesWatch.launchPodWatch().join();
  }

  private static class PreInitializedWatch extends KubernetesWatch {
    private final Watch<V1Pod> preInitPodWatch;
    private final int maxInitCount;
    private int initWatchCount = 0;
    private int startWatchCount = 0;

    public PreInitializedWatch(KubeConfig kubeConfig,
        RawPodWatchEventListener podWatchEventListener,
        Watch<V1Pod> preInitPodWatch,
        PodWatchParams podWatchParams,
        int maxInitCount) {
      super(kubeConfig, podWatchEventListener, podWatchParams);
      requireNonNull(preInitPodWatch, "pre init pod watch must not be null");
      this.preInitPodWatch = preInitPodWatch;
      this.maxInitCount = maxInitCount;
    }

    @Override
    protected void initializePodWatch() {
      this.setPodWatch(preInitPodWatch);
      if (initWatchCount >= maxInitCount) {
        logger.debug("Requesting shutdowns as max init count was reached, init-count: " + initWatchCount);
        this.requestShutdown();
      }
      initWatchCount++;
    }

    @Override
    protected void startPodWatch() throws IOException {
      startWatchCount++;
      super.startPodWatch();
    }

    public int getMaxInitCount() {
      return maxInitCount;
    }

    public int getInitWatchCount() {
      return initWatchCount;
    }

    public int getStartWatchCount() {
      return startWatchCount;
    }
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