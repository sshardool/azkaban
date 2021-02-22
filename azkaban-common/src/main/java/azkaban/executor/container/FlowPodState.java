package azkaban.executor.container;

import azkaban.executor.ExecutableFlow;

public class FlowPodState {
  private final AzPodStatus podStatus;
  private final ExecutableFlow executableFlow;

  public FlowPodState(AzPodStatus podStatus, ExecutableFlow executableFlow) {
    this.podStatus = podStatus;
    this.executableFlow = executableFlow;
  }

  public AzPodStatus getPodStatus() {
    return podStatus;
  }

  public ExecutableFlow getExecutableFlow() {
    return executableFlow;
  }
}
