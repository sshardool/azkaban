package azkaban.executor.container;

import static java.util.Objects.requireNonNull;

import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodCondition;
import io.kubernetes.client.openapi.models.V1PodStatus;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  AZ_POD_SCHEDULED,
 *  PodScheduled is true, other conditions are missing or false, no init-containers running or
 *  completed.
 *
 *  AZ_POD_INIT_CONTAINERS_RUNNING,
 *  PodScheduled is true, Initialized is false, at least 1 init-container running.
 *
 *  AZ_POD_APP_CONTAINERS_STARTING,
 *  Initialized is true, at least 1 application container running.
 *
 *  AZ_POD_READY,
 *  ContainersReady is true, Ready is true. In absence of readiness gates both of these
 *  conditions are identical. We can consider splitting the AZ_POD_READY into 2 separate values
 *  if readiness gates are introduced and need to be accounted for separately.
 *
 *  AZ_POD_COMPLETED,
 *  Phase is Succeeded.
 *
 *  AZ_POD_INIT_ERROR,
 *  Phase is Failed. At least 1 init-container terminated with failure.
 *
 *  AZ_POD_APP_ERROR,
 *  Phase is Failed. At least 1 application container terminated with failure.
 *
 *  AZ_POD_UNEXPECTED
 *  An event that can't be classified into any other AzPodStatus. These should be logged and
 *  tracked.
 *
 */
public class AzPodStatusExtractor {
  private static final Logger logger = LoggerFactory.getLogger(AzPodStatusExtractor.class);

  private final Watch.Response<V1Pod> podWatchResponse;
  private final V1Pod v1Pod;
  private final V1PodStatus v1PodStatus;
  private final List<V1PodCondition> podConditions;
  private Optional<V1PodCondition> scheduledCondition = Optional.empty();
  private Optional<V1PodCondition> containersReadyCondition = Optional.empty();
  private Optional<V1PodCondition> initializedCondition = Optional.empty();
  private Optional<V1PodCondition> readyCondition = Optional.empty();
  private PodPhase podPhase;

  public AzPodStatusExtractor(Response<V1Pod> podWatchResponse) {
    requireNonNull(podWatchResponse, "pod watch response must not be null");
    requireNonNull(podWatchResponse.object, "watch v1Pod must not be null");
    this.podWatchResponse = podWatchResponse;
    this.v1Pod = (V1Pod)podWatchResponse.object;

    requireNonNull(v1Pod.getStatus(), "pod status must not be null");
    requireNonNull(v1Pod.getStatus().getConditions(), "pod status conditions must not be null");
    requireNonNull(v1Pod.getStatus().getPhase(), "pod phase must not be null");

    this.v1PodStatus = v1Pod.getStatus();
    this.podConditions = v1PodStatus.getConditions();

    extractConditions();
    extractPhase();
  }

  private void extractConditions() {
    Map<String, V1PodCondition> conditionMap = new HashMap<>();
    this.podConditions.stream().forEach(
        condition ->
            conditionMap.put(condition.getType(), condition));
  scheduledCondition = Optional.ofNullable(conditionMap.remove(PodCondition.PodScheduled.name()));
  containersReadyCondition = Optional.ofNullable(conditionMap.remove(PodCondition.ContainersReady.name()));
  initializedCondition = Optional.ofNullable(conditionMap.remove(PodCondition.Initialized.name()));
  readyCondition = Optional.ofNullable(conditionMap.remove(PodCondition.Ready.name()));

  conditionMap.keySet().stream().forEach(type -> logger.warn("Unexpected condition of type: " + type));
  }

  private void extractPhase() {
    requireNonNull(v1PodStatus.getPhase(), "pod status phase must not be null");
    // This will throw an IllegalArgumentException in case of an unexpected phase name.
    podPhase = PodPhase.valueOf(v1PodStatus.getPhase());
  }

  /**
   *
   * @param watchEvent
   * @return
   */
  public AzPodStatus azPodStatusFromEvent() {


    //Optional<V1PodCondition> scheduledCondition

    return null;
  }

  private static enum PodCondition {
    PodScheduled,
    ContainersReady,
    Initialized,
    Ready
  }

  private static enum PodPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown
  }
}
