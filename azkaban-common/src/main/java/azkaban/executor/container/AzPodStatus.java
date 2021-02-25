package azkaban.executor.container;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The Enum represents the different stages of the pod lifecycle.
 * While the Kubernetes API can provide very granular information about the current states
 * of various aspects of a POD, it doesn't quite provide any state-machine like representation of
 * POD life-cycle at a good enough granularity for use within Azkaban. <br>
 *   https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
 *
 * For example, a Pod Phase is very coarse and doesn't convey information regarding
 * pod-scheduling and init-container completions.
 * On the other hand Pod Conditions array along with the container statuses provides detailed
 * information down the states and timestamps on individual containers but there is no direct
 * mechanism provided for distilling this information to a 'state' of generic pod (irrespective of
 * what or how many containers are included in the pod)
 *
 * Defining our own set of states also gives us the flexibility of splitting or merging states
 * based on how our needs evolve in future.
 *
 *   Unset
 *     |
 *  Scheduled --> InitContainersRunning --> AppContainersStarting --> Ready --> Completed
 *     |                   |                   |                        |          |
 *     |                   |                   |                        |          |
 *     '--> InitFailure <--'                   '-----> AppFailure <-----'          '--> Deleted
 *
 */
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
  AZ_POD_DELETED,
  AZ_POD_UNEXPECTED;
}
