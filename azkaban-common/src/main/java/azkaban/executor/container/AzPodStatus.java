package azkaban.executor.container;

//
//
//  Unset
//    |
// Scheduled --> InitContainersRunning --> AppContainersStarting --> Ready --> Completed
//    |                   |                   |                        |
//    |                   |                   |                        |
//    '--> InitFailure <--'                   '-----> AppFailure <-----'
//
public enum AzPodStatus {
  AZ_POD_UNSET,
  AZ_POD_REQUESTED,
  AZ_POD_SCHEDULED,
  AZ_POD_INIT_CONTAINERS_RUNNING,
  AZ_POD_APP_CONTAINERS_STARTING,
  AZ_POD_READY,
  AZ_POD_COMPLETED,
  AZ_POD_INIT_FAILURE,
  AZ_POD_APP_FAILURE,
  AZ_POD_UNEXPECTED;
}
