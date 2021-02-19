package azkaban.executor.container;

//
// Dispatched --> InitContainersRunning --> AppContainersRunning --> Completed
//    |                  |
//    |                  |
//    '-->  InitError <--'
//
public enum AzPodState {
  AZ_POD_UNSET,
  AZ_POD_DISPATCHED,
  AZ_POD_INIT_CONTAINERS_RUNNING,
  AZ_POD_APP_CONTAINERS_RUNNING,
  AZ_POD_COMPLETED,
  AZ_POD_INIT_ERROR,
  AZ_POD_APP_ERROR
}
