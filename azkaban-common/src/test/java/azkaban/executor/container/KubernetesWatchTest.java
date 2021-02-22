package azkaban.executor.container;

import static java.util.Objects.requireNonNull;

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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesWatchTest {
  private static final Logger logger = LoggerFactory.getLogger(KubernetesWatchTest.class);

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

  // todo: sanitize and package json files as resources
  private static String KUBE_CONFIG_PATH = "/Users/sshardoo/.kube/proxy-config";
  private static String JSON_EVENTS_FILE_PATH = "/Users/sshardoo/source/misc/sample3-pods.json";
  private KubeConfig kubeConfig;
  private KubernetesWatch kubeWatch;

  @Before
  public void setUp() throws Exception {
    this.kubeConfig = KubeConfig.loadKubeConfig(
        Files.newBufferedReader(Paths.get(KUBE_CONFIG_PATH), Charset.defaultCharset()));
    this.kubeWatch = new KubernetesWatch(kubeConfig, new MockPodWatchEventListener());
  }

  @Test
  public void testWithMockedEvents() throws Exception {
    ApiClient apiClient = ClientBuilder.kubeconfig(kubeConfig).build();
    FileBackedWatch<V1Pod> fileBackedWatch = new FileBackedWatch<>(
        apiClient.getJSON(),
        new TypeToken<Response<V1Pod>>() {}.getType(),
        Paths.get(JSON_EVENTS_FILE_PATH));
    kubeWatch.processPodWatchEvents(fileBackedWatch);
  }

  @Test
  public void testPodWatch() throws Exception {
    kubeWatch.initializePodWatch();
    kubeWatch.startPodWatch();
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