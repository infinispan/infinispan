package org.infinispan.factories;

/**
 * Holder for known named component names.  To be used with {@link org.infinispan.factories.annotations.ComponentName}
 * annotation.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class KnownComponentNames {
   public static final String ASYNC_TRANSPORT_EXECUTOR = "org.infinispan.executors.transport";
   public static final String ASYNC_NOTIFICATION_EXECUTOR = "org.infinispan.executors.notification";
   public static final String EVICTION_SCHEDULED_EXECUTOR = "org.infinispan.executors.eviction";
   public static final String ASYNC_REPLICATION_QUEUE_EXECUTOR = "org.infinispan.executors.replicationQueue";
}
