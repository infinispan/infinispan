package org.horizon.factories;

/**
 * Holder for known named component names.  To be used with {@link org.horizon.factories.annotations.ComponentName}
 * annotation.
 *
 * @author Manik Surtani
 * @since 1.0
 */
public class KnownComponentNames {
   public static final String ASYNC_SERIALIZATION_EXECUTOR = "org.horizon.executors.serialization";
   public static final String ASYNC_NOTIFICATION_EXECUTOR = "org.horizon.executors.notification";
   public static final String EVICTION_SCHEDULED_EXECUTOR = "org.horizon.executors.eviction";
   public static final String ASYNC_REPLICATION_QUEUE_EXECUTOR = "org.horizon.executors.replicationQueue";
}
