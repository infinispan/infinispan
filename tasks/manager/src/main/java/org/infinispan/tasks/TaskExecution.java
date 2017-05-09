package org.infinispan.tasks;

import java.time.Instant;
import java.util.Optional;

/**
 * TaskExecution. Contains information about a running task
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public interface TaskExecution {
   /**
    * Returns the name of the task
    */
   String getName();

   /**
    * Returns the time when the task was started
    */
   Instant getStart();

   /**
    * An optional name of the principal who has executed this task. If the task was triggered
    * internally, this method will return an empty {@link Optional}
    */
   Optional<String> getWho();

   /**
    * An optional context to which the task was applied. Usually the name of a cache
    */
   Optional<String> getWhat();

   /**
    * The node/address where the task was initiated
    */
   String getWhere();
}
