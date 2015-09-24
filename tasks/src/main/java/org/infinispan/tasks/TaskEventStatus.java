package org.infinispan.tasks;

/**
 * TaskEventStatus.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public enum TaskEventStatus {
   PENDING,
   RUNNING,
   SUCCESS,
   ERROR,
   CANCELLED
}
