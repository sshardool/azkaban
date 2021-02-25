package azkaban.executor.container;

/**
 * Provides callback methods for processing of {@code AzPodStatus} states.
 * Each of the methods here directly corresponds to an enum value in{@code AzPodStatus}
 *
 * Method implementations are expected to be idempotent as it's possible to receive different
 * events which all map to the same enum value in {@code AzPodStatus}
 *
 */
public interface AzPodStatusListener {
  default void onPodRequested(AzPodStatusMetadata event) {}
  default void onPodScheduled(AzPodStatusMetadata event) {}
  default void onPodInitContainersRunning(AzPodStatusMetadata event) {}
  default void onPodAppContainersStarting(AzPodStatusMetadata event) {}
  default void onPodReady(AzPodStatusMetadata event) {}
  default void onPodCompleted(AzPodStatusMetadata event) {}
  default void onPodInitFailure(AzPodStatusMetadata event) {}
  default void onPodAppFailure(AzPodStatusMetadata event) {}
  default void onPodUnexpected(AzPodStatusMetadata event) {}
}
