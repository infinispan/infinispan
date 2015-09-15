package org.infinispan.tasks;

import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface TaskManager {

   /**
    * Executes the named task, passing an optional cache and a map of named parameters.
    *
    * @param taskName
    * @param context
    * @return
    */
   <T> CompletableFuture<T> runTask(String taskName, TaskContext context);

   /**
    * Retrieves the history of task executions for all tasks and two instants in time
    *
    * @param fromInstant the beginning {@link Instant} from which to return the history
    * @param toInstant the finish {@link Instant} from which to return the history
    * @return a list of {@link TaskEvent} elements
    */
   List<TaskEvent> getTaskHistory(Instant fromInstant, Instant toInstant);

   /**
    * Retrieves the history of task executions for the last period
    *
    * @param temporalAmount the amount of time since the current instance
    * @return a list of {@link TaskEvent} elements
    */
   List<TaskEvent> getTaskHistory(TemporalAmount temporalAmount);


   /**
    * Retrieves the history of task executions for a given named task and two instants in time
    *
    * @param taskName
    * @param fromInstant
    * @param toInstant
    * @return a list of {@link TaskEvent} elements
    */
   List<TaskEvent> getTaskHistory(String taskName, Instant fromInstant, Instant toInstant);

   /**
    * Retrieves the history of task executions for a given named task for the last period
    *
    * @param taskName
    * @param temporalAmount the amount of time since the current instance
    * @return a list of {@link TaskEvent}
    */
   List<TaskEvent> getTaskHistory(String taskName, TemporalAmount temporalAmount);

   /**
    * Clears the task history
    */
   void clearTaskHistory();
}
