package org.infinispan.tasks.spi;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.tasks.Task;

/**
 * Extends the {@link TaskEngine} interface to include additional methods allowing a task
 * engine to have non blocking methods where as the mirrored methods on {@link TaskEngine}
 * would block the invoking thread.
 */
public interface NonBlockingTaskEngine extends TaskEngine {
   /**
    * Returns the list of tasks managed by this engine. This method will return immediately and the returned stage
    * will now or some point in the future be completed with the list of tasks or an exception if something went wrong.
    *
    * @return stage containing the list of tasks when it is retrieved or an error
    */
   CompletionStage<List<Task>> getTasksAsync();

   /**
    * Returns whether this task engine knows about a specified named task. This method will return immediately and
    * the returned stage will now or some point in the future be completed with a Boolean whether this engine can
    * perform this task or an exception if something went wrong
    *
    * @param taskName the task to check
    * @return stage containing if the task can be handled or an error
    */
   CompletionStage<Boolean> handlesAsync(String taskName);
}
