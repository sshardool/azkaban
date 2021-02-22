package azkaban.executor.container;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodCondition;
import io.kubernetes.client.openapi.models.V1PodStatus;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
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
 *  conditions are identical. We can consider splitting the AZ_POD_READY into 2 separate states
 *  if readiness gates are introduced and need to be accounted for in future.
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
  private Optional<PodConditionStatus> scheduledConditionStatus = Optional.empty();
  private Optional<PodConditionStatus> containersReadyConditionStatus = Optional.empty();
  private Optional<PodConditionStatus> initializedConditionStatus = Optional.empty();
  private Optional<PodConditionStatus> readyConditionStatus = Optional.empty();
  private PodPhase podPhase;

  public AzPodStatusExtractor(Response<V1Pod> podWatchResponse) {
    requireNonNull(podWatchResponse, "pod watch response must not be null");
    requireNonNull(podWatchResponse.object, "watch v1Pod must not be null");
    this.podWatchResponse = podWatchResponse;
    this.v1Pod = (V1Pod)podWatchResponse.object;

    requireNonNull(v1Pod.getStatus(), "pod status must not be null");
    requireNonNull(v1Pod.getStatus().getPhase(), "pod phase must not be null");
    this.v1PodStatus = v1Pod.getStatus();
    this.podConditions = v1Pod.getStatus().getConditions();

    if (podConditions != null) {
      extractConditions();
      extractConditionStatuses();
    }
    extractPhase();
  }

  private void extractConditions() {
    requireNonNull(podConditions, "pod status conditions must not be null");
    Map<String, V1PodCondition> conditionMap = new HashMap<>();
    this.podConditions.stream().forEach(
        condition ->
            conditionMap.put(condition.getType(), condition));
  this.scheduledCondition =
      Optional.ofNullable(conditionMap.remove(PodCondition.PodScheduled.name()));
  this.containersReadyCondition =
      Optional.ofNullable(conditionMap.remove(PodCondition.ContainersReady.name()));
  this.initializedCondition =
      Optional.ofNullable(conditionMap.remove(PodCondition.Initialized.name()));
  this.readyCondition = Optional.ofNullable(conditionMap.remove(PodCondition.Ready.name()));

  conditionMap.keySet().stream().forEach(type -> logger.warn("Unexpected condition of type: " + type));
  }

  private void extractConditionStatuses() {
    this.scheduledCondition.ifPresent(cond -> {
      requireNonNull(cond.getStatus());
      this.scheduledConditionStatus = Optional.of(PodConditionStatus.valueOf(cond.getStatus()));
    });
    this.containersReadyCondition.ifPresent(cond -> {
      requireNonNull(cond.getStatus());
      this.containersReadyConditionStatus = Optional
          .of(PodConditionStatus.valueOf(cond.getStatus()));
    });
    this.initializedCondition.ifPresent(cond -> {
      requireNonNull(cond.getStatus());
      this.initializedConditionStatus =
          Optional.of(PodConditionStatus.valueOf(cond.getStatus()));
    });
    this.readyCondition.ifPresent(cond -> {
      requireNonNull(cond.getStatus());
      this.readyConditionStatus = Optional.of(PodConditionStatus.valueOf(cond.getStatus()));
    });
  }

  private void extractPhase() {
    requireNonNull(this.v1PodStatus.getPhase(), "pod status phase must not be null");
    // This will throw an IllegalArgumentException in case of an unexpected phase name.
    this.podPhase = PodPhase.valueOf(v1PodStatus.getPhase());
  }

  private boolean checkForAzPodRequested() {
    // Scheduled conditions should either not be present or be false
    if (scheduledConditionStatus.isPresent() &&
        scheduledConditionStatus.get() == PodConditionStatus.True) {
      logger.debug("PodRequested is false as scheduled conditions is true");
      return false;
    }
    logger.debug("PodRequested is true");
    return true;
  }

  private boolean checkForAzPodScheduled() {
    // Pod must have been scheduled
    if (!scheduledConditionStatus.isPresent()) {
      logger.debug("PodScheduled false as scheduled condition is not present");
      return false;
    }
    if (scheduledConditionStatus.get() != PodConditionStatus.True) {
      logger.debug("PodScheduled false as scheduled condition is not true");
      return false;
    }
    // Initialized condition is not present
    if (!initializedCondition.isPresent()) {
      logger.debug("PodScheduled true as initialized condition is not present");
      return true;
    }
    // Initialized condition is not true
    if (initializedConditionStatus.get() == PodConditionStatus.True) {
      logger.debug("PodScheduled false as initialized condition is true");
      return false;
    }
    // No init-containers should be running
    List<V1ContainerStatus> initContainerStatuses = v1PodStatus.getInitContainerStatuses();
    if (initContainerStatuses == null || initContainerStatuses.isEmpty()) {
      logger.debug("PodScheduled is true as init container status is null or empty");
      return true;
    }
    boolean anyContainerRunning = initContainerStatuses.stream().anyMatch(status ->
        (status.getState().getRunning() != null &&
            status.getState().getRunning().getStartedAt() != null) ||
            (status.getState().getTerminated() != null &&
                status.getState().getTerminated().getFinishedAt() != null));
    if (anyContainerRunning) {
      logger.debug("PodScheduled is false as an init container is running");
      return false;
    }
    logger.debug("PodScheduled is true");
    return true;
  }

  private boolean checkForAzPodInitContainersRunning() {
    // Pod must have scheduled
    if (!scheduledConditionStatus.isPresent() ||
        scheduledConditionStatus.get() != PodConditionStatus.True) {
      logger.debug("InitRunning false as scheduled condition is not present or not true");
      return false;
    }
    // Initialization must have started, i.e condition should exist
    if (!initializedConditionStatus.isPresent()) {
      logger.debug("InitRunning false as initialized conditions is not present");
      return false;
    }
    // Initialization must not be complete
    if (initializedConditionStatus.get() == PodConditionStatus.True) {
      logger.debug("InitRunning false as initialized condition is true");
      return false;
    }
    // todo: at least 1 init container running
    logger.debug("InitRunning is true");
    return true;
  }

  private boolean checkForAzPodAppContainerStarting() {
    // Pod must have been initialized
    if (!initializedConditionStatus.isPresent() ||
        initializedConditionStatus.get() != PodConditionStatus.True) {
      logger.debug("ContainerStarting false as initialized condition is not present or not true");
      return false;
    }
    // ContainersReady condition will not be True and all application containers should be waiting
    List<V1ContainerStatus> containerStatuses = v1PodStatus.getContainerStatuses();
    if (containerStatuses == null || containerStatuses.isEmpty()) {
      logger.debug("ContainerStarting false as container status is null or empty");
      return false;
    }
    boolean allContainersWaiting = containerStatuses.stream().allMatch(status ->
        status.getState().getWaiting() != null && status.getStarted() == false);
    if (!allContainersWaiting) {
      logger.debug("ContainerStarting false as all containers are not waiting");
      return false;
    }
    logger.debug("ContainerStarting is true");
    return true;
  }

  private boolean checkForAzPodReady() {
    // ContainersReady condition must be True
    if (!containersReadyConditionStatus.isPresent() ||
        containersReadyConditionStatus.get() != PodConditionStatus.True) {
      logger.debug("PodReady false as container-ready condition is not present or not true");
      return false;
    }
    // All application containers should be running
    List<V1ContainerStatus> containerStatuses = v1PodStatus.getContainerStatuses();
    if (containerStatuses == null || containerStatuses.isEmpty()) {
      logger.debug("PodReady false as container status is null or empty");
      return false;
    }
    boolean allContainersRunning = containerStatuses.stream().allMatch(status ->
        status.getState().getRunning() != null &&
            status.getState().getRunning().getStartedAt() != null);
    if (!allContainersRunning) {
      logger.debug("PodReady false as all containers are not running");
      return false;
    }
    logger.debug("PodReady is true");
    return true;
  }

  private boolean checkForAzPodCompleted() {
    // Phase should be succeeded
    if(podPhase != PodPhase.Succeeded) {
      logger.debug("PodCompleted is false as phase is not succeeded");
      return false;
    }
    logger.debug("PodCompleted is true");
    return true;
  }

  private boolean checkForAzPodInitFailure() {
    // Phase must be failed.
      if (podPhase != PodPhase.Failed) {
        logger.debug("InitFailed is false and phase is not failed");
        return false;
    }
    // Initalized conditions should not be true
    if (initializedConditionStatus.isPresent() &&
        initializedConditionStatus.get() == PodConditionStatus.True) {
      logger.debug("InitFailed is failed as initialized conditions is not present or true");
      return false;
    }

    // There must be at least 1 failed init container
    List<V1ContainerStatus> initContainerStatuses = v1PodStatus.getInitContainerStatuses();
    if (initContainerStatuses == null || initContainerStatuses.isEmpty()) {
      logger.debug("InitFailed is false as init container status is null or empty");
      return false;
    }
    boolean anyContainerFailed = initContainerStatuses.stream().anyMatch(status ->
        status.getState().getTerminated() != null &&
            (status.getState().getTerminated().getExitCode() == null ||
                status.getState().getTerminated().getExitCode() != 0));
    if (!anyContainerFailed) {
      logger.debug("InitFailed is false as as all init container are terminated with exit code 0");
      return false;
    }
    logger.debug("InitFailed is true");
    return true;
  }

  private boolean checkForAzPodAppFailure() {
    // Phase must be failed.
    if (podPhase != PodPhase.Failed) {
      logger.debug("AppFailed is false and phase is not failed");
      return false;
    }
    // Initialized condition should  be true
    if (!initializedConditionStatus.isPresent() ||
        initializedConditionStatus.get() != PodConditionStatus.True) {
      logger.debug("AppFailed is failed as initialized conditions is not present or not true");
      return false;
    }
    // There must be at least 1 failed app container
    List<V1ContainerStatus> containerStatuses = v1PodStatus.getInitContainerStatuses();
    if (containerStatuses == null || containerStatuses.isEmpty()) {
      logger.debug("AppFailed is false as container status is null or empty");
      return false;
    }
    boolean anyContainerFailed = containerStatuses.stream().anyMatch(status ->
        status.getState().getTerminated() != null &&
            (status.getState().getTerminated().getExitCode() == null ||
                status.getState().getTerminated().getExitCode() != 0));
    if (!anyContainerFailed) {
      logger.debug("AppFailed is false as as no container terminated with non-zero exit code");
      return false;
    }
    logger.debug("AppFailed is true");
    return true;
  }

  /**
   *
   * @return
   */
  public AzPodStatus createAzPodStatus() {
//    final Map<AzPodStatus, BooleanSupplier> methodAzPodStatusMap = ImmutableMap.builder()
//        .put(AzPodStatus.AZ_POD_SCHEDULED, this::checkForAzPodScheduled)
//        .build();
    if (checkForAzPodScheduled()) {
      return AzPodStatus.AZ_POD_REQUESTED;
    }
    if (checkForAzPodScheduled()) {
      return AzPodStatus.AZ_POD_SCHEDULED;
    }
    if (checkForAzPodInitContainersRunning()) {
      return AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING;
    }
    if (checkForAzPodAppContainerStarting()) {
      return AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING;
    }
    if (checkForAzPodReady()) {
      return AzPodStatus.AZ_POD_READY;
    }
    if (checkForAzPodCompleted()) {
      return AzPodStatus.AZ_POD_COMPLETED;
    }
    if (checkForAzPodInitFailure()) {
      return AzPodStatus.AZ_POD_INIT_FAILURE;
    }
    if (checkForAzPodAppFailure()) {
      return AzPodStatus.AZ_POD_APP_FAILURE;
    }
    return AzPodStatus.AZ_POD_UNEXPECTED;
  }

  /**
   * Convenience method to create AzPodStatus from event in a single call.
   *
   * @param event
   * @return
   */
  public static AzPodStatusWithEvent azPodStatusFromEvent(Watch.Response<V1Pod> event) {
    AzPodStatusExtractor extractor = new AzPodStatusExtractor(event);
    return new AzPodStatusWithEvent(extractor.createAzPodStatus(), event);
  }

  private enum PodCondition {
    PodScheduled,
    ContainersReady,
    Initialized,
    Ready
  }

  private enum PodConditionStatus {
    True,
    False,
    Unknown
  }

  private enum PodPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown
  }
}
