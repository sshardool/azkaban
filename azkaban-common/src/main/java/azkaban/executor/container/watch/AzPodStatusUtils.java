package azkaban.executor.container.watch;

import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_APP_FAILURE;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_COMPLETED;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_DELETED;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_INIT_FAILURE;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_READY;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_REQUESTED;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_SCHEDULED;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_UNEXPECTED;
import static azkaban.executor.container.watch.AzPodStatus.AZ_POD_UNSET;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AzPodStatusUtils {
  private static final Set<AzPodStatus> finalStatuses = EnumSet.of(
      AZ_POD_COMPLETED,
      AZ_POD_INIT_FAILURE,
      AZ_POD_APP_FAILURE,
      AZ_POD_DELETED,
      AZ_POD_UNEXPECTED
  );
  private static final Map<AzPodStatus, Set<AzPodStatus>> invalidPreviousStatusMap =
      buildInvalidStatusMap();

  private static Map<AzPodStatus, Set<AzPodStatus>> buildInvalidStatusMap() {
    ImmutableMap.Builder builder = ImmutableMap.builder()
        .put(AZ_POD_READY, finalStatuses);

    Set<AzPodStatus> mutableInvalidStatuses = new HashSet<>(finalStatuses);
    mutableInvalidStatuses.add(AZ_POD_READY);
    builder.put(AZ_POD_APP_CONTAINERS_STARTING, EnumSet.copyOf(mutableInvalidStatuses));

    mutableInvalidStatuses.add(AZ_POD_APP_CONTAINERS_STARTING);
    builder.put(AZ_POD_INIT_CONTAINERS_RUNNING, EnumSet.copyOf(mutableInvalidStatuses));

    mutableInvalidStatuses.add(AZ_POD_INIT_CONTAINERS_RUNNING);
    builder.put(AZ_POD_SCHEDULED, EnumSet.copyOf(mutableInvalidStatuses));

    mutableInvalidStatuses.add(AZ_POD_SCHEDULED);
    builder.put(AZ_POD_REQUESTED, EnumSet.copyOf(mutableInvalidStatuses));

    mutableInvalidStatuses.add(AZ_POD_REQUESTED);
    builder.put(AZ_POD_UNSET, EnumSet.copyOf(mutableInvalidStatuses));

    builder.put(AZ_POD_APP_FAILURE, EnumSet.of(AZ_POD_INIT_FAILURE, AZ_POD_DELETED));
    builder.put(AZ_POD_INIT_FAILURE, EnumSet.of(AZ_POD_APP_FAILURE, AZ_POD_DELETED));
    builder.put(AZ_POD_UNEXPECTED, EnumSet.of(AZ_POD_DELETED));
    builder.put(AZ_POD_DELETED, ImmutableSet.of());
    builder.put(AZ_POD_UNSET, EnumSet.allOf(AzPodStatus.class));
    return builder.build();
  }

  public static boolean isTrasitionValid(AzPodStatus oldStatus, AzPodStatus newStatus) {
    Set<AzPodStatus> invalidSet = invalidPreviousStatusMap
        .getOrDefault(oldStatus, ImmutableSet.of());
    return !invalidSet.contains(newStatus);
  }
}
