package azkaban.executor.container;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionControllerUtils;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides callback implementations of {@code AzPodStatusListener} for
 * (1) Updating WebServer and database state of flows based on Pod status
 * (2) Driving pod lifecycle actions, such as deleting flow pods in a final state.
 */
public class FlowStatusUpdatingListener implements AzPodStatusListener {
  // Skip updates if there is already a current pod state for the flow which is same as the new one
  // Synchronize on a unique flow specific value, i.e updates to a single flow are synchronized
  // Account for no transitions out of final-states except for pod-deletion
  // Cleanup of status map and add note about the size of status map
  private static final Logger logger = LoggerFactory.getLogger(FlowStatusUpdatingListener.class);

  // Note that both KubernetesContainerizedImpl and ExecutorLoader are not thread-safe and
  // access to them needs to be synchronized.
  private final ContainerizedImpl containerizedImpl;
  private final ExecutorLoader executorLoader;
  private final ConcurrentMap<String, AzPodStatusMetadata> podStatusMap = new ConcurrentHashMap<>();

  @Inject
  public FlowStatusUpdatingListener(ContainerizedImpl containerizedImpl,
      ExecutorLoader executorLoader) {
    requireNonNull(containerizedImpl, "container implementation must not be null");
    requireNonNull(executorLoader, "executor loader must not be null");
    this.containerizedImpl = containerizedImpl;
    this.executorLoader = executorLoader;
  }

  private void validateTransition(AzPodStatusMetadata event) {
    AzPodStatus currentStatus = AzPodStatus.AZ_POD_UNSET;
    if (podStatusMap.containsKey(event.getPodName())) {
      currentStatus = podStatusMap.get(event.getPodName()).getAzPodStatus();
    }
    logger.debug(format("Transition requested from %s -> %s, for pod %s",
        currentStatus,
        event.getAzPodStatus(),
        event.getPodName()));

    if (!AzPodStatusUtils.isTrasitionValid(currentStatus, event.getAzPodStatus())) {
      IllegalStateException ise = new IllegalStateException(
          format("Pod status transition is not supported %s -> %s, for pod %s",
              currentStatus,
              event.getAzPodStatus(),
              event.getPodName()));
      logger.error("Unsupported state transition.", ise);
      throw ise;
    }
  }

  /**
   * Includes any validations needed on event that are common to all {@code AzPodStatus}
   * This is also responsible for updating the status map in case, creating a new entry if needed.
   *
   * @param event
   */
  private void validateAndPreProcess(AzPodStatusMetadata event) {
    // Confirm that event is indeed a for a FlowContainer and has the corresponding metadata
    if (!event.getFlowPodMetadata().isPresent()) {
      IllegalStateException ise = new IllegalStateException(
          format("Flow metadata is not present for pod %s", event.getPodName()));
      logger.error("Pod is likely not a Flow Container.", ise);
      throw ise;
    }
    validateTransition(event);
    podStatusMap.putIfAbsent(event.getPodName(), event);
  }

  private boolean isUpdatedPodStatusDistinct(AzPodStatusMetadata event) {
    AzPodStatus currentStatus = AzPodStatus.AZ_POD_UNSET;
    if (podStatusMap.containsKey(event.getPodName())) {
      currentStatus = podStatusMap.get(event.getPodName()).getAzPodStatus();
    }
    boolean shouldSkip = currentStatus == event.getAzPodStatus();
    if (shouldSkip) {
      logger.info(format("Event pod status is same as current %s, for pod %s."
          +" Any updates will be skipped.", currentStatus, event.getPodName()));
    }
    return !shouldSkip;
  }

  private void updatePodStatus(AzPodStatusMetadata event) {
    podStatusMap.put(event.getPodName(), event);
    logger.debug(format("Updated status to %s, for pod %s", event.getAzPodStatus(), event.getPodName()));
  }

  /**
   * Apply the boolean function {@code expectedStatusMatcher} to execution-id and if it returns true
   * then finalize he status of the flow in db.
   * This can be used for finalzing the flow based on whether the current Flow Status has a
   * specific value or is within a contained in a given set of values.
   *
   * <p> Note:
   * Unfortunately most (if not all) of the Flow status updates in Azkaban completely disregard
   * the Flow lifecycle state-machine and {@link ExecutionControllerUtils.finalizeFlow} used
   * within this method is no exception. It will simply finalize the flow (as failed) even if the
   * flow status is in a finalized state in the Db. <br>
   * This could be a problem for this listener implementation as occasionally more than one thread
   * could try to update the state of a the same flow in db. Although it's not any worse than how
   * the rest of Azkaban already behaves, we should fix the behavior at least during the
   * finalization of flows.
   * todo: Add utility method to atomically test-and-finalize a flow from non-final to failed state.
   *       Rationale is explained above.
   *
   * @implNote Flow status check and update is not atomic, details above.
   * @param event
   * @param expectedStatusMatcher
   * @return
   */
  private azkaban.executor.Status compareAndFinalizeFlowStatus(AzPodStatusMetadata event,
      Function<Status, Boolean> expectedStatusMatcher) {
    requireNonNull(expectedStatusMatcher, "flow matcher must not be null");

    int executionId = Integer.parseInt(event.getFlowPodMetadata().get().getExecutionId());
    ExecutableFlow executableFlow = null;
    try {
      executableFlow = executorLoader.fetchExecutableFlow(executionId);
    } catch (ExecutorManagerException e) {
      String message = format("Exception while fetching executable flow for pod %s",
          event.getPodName());
      logger.error(message, e);
      throw new RuntimeException(message, e);
    }
    checkState(executableFlow != null, "executable flow must not be null");
    azkaban.executor.Status originalStatus = executableFlow.getStatus();

    if (!expectedStatusMatcher.apply(originalStatus)) {
      logger.info(format(
          "Flow for pod %s does not have the expected status in database and will be finalized.",
          event.getPodName()));
      ExecutionControllerUtils.finalizeFlow(executorLoader, null, null, null, null);
    }
    return originalStatus;
  }

  private void deleteFlowContainer(AzPodStatusMetadata event) {
    try {
      containerizedImpl.deleteContainer(
          Integer.parseInt(
              event.getFlowPodMetadata().get().getExecutionId()));
    } catch (ExecutorManagerException e) {
      String message = format("Exception while deleting flow container.");
      logger.error(message, e);
      throw new RuntimeException(message, e);
    } catch (NumberFormatException ne) {
      String message = format("Flow metadata execution id is not a valid integer %s",
          event.getFlowPodMetadata().get().getExecutionId());
      throw new RuntimeException(message, ne);
    }
  }

  @Override
  public void onPodCompleted(AzPodStatusMetadata event) {
    requireNonNull(event, "event must not be null");
    validateAndPreProcess(event);
    if (!isUpdatedPodStatusDistinct(event)) {
      return;
    }
    azkaban.executor.Status originalFlowStatus = compareAndFinalizeFlowStatus(event,
        azkaban.executor.Status::isStatusFinished);
    if (!Status.isStatusFinished(originalFlowStatus)) {
      logger.warn(format("Flow for pod %s was in the non-final state %s and was finalized",
          event.getPodName(), originalFlowStatus));
    }
    deleteFlowContainer(event);
    updatePodStatus(event);
  }

  private UnsupportedOperationException createUnsupportedException(AzPodStatusMetadata event) {
    String message = format("Callback for Pod status %s is not yet implemented. Pod name %s",
        event.getAzPodStatus(),
        event.getPodName());
    return new UnsupportedOperationException(message);
  }

  @Override
  public void onPodRequested(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }

  @Override
  public void onPodScheduled(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }

  @Override
  public void onPodInitContainersRunning(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }

  @Override
  public void onPodAppContainersStarting(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }

  @Override
  public void onPodReady(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }

  @Override
  public void onPodInitFailure(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }

  @Override
  public void onPodAppFailure(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }

  @Override
  public void onPodUnexpected(AzPodStatusMetadata event) {
    logger.warn("Unsupported method.", createUnsupportedException(event));
  }
}
