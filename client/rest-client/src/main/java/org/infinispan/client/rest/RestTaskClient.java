package org.infinispan.client.rest;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/
public interface RestTaskClient {

   /**
    * Task type filter definition.
    */
   enum ResultType {
      /**
       * User created tasks
       */
      USER,
      /**
       * All tasks, including admin tasks, that contains name starting with '@@'
       */
      ALL
   }

   /**
    * Retrieves a list of tasks from the server
    * @param resultType the type of task to return
    */
   CompletionStage<RestResponse> list(ResultType resultType);

   /**
    * Executes a task with the supplied parameters. Currently only supports String values
    */
   CompletionStage<RestResponse> exec(String taskName, String cacheName, Map<String, ?> parameters);

   /**
    * Uploads a script
    */
   CompletionStage<RestResponse> uploadScript(String taskName, RestEntity script);

   /**
    * Downloads a script
    */
   CompletionStage<RestResponse> downloadScript(String taskName);

   /**
    * Executes a task without parameters
    */
   default CompletionStage<RestResponse> exec(String taskName) {
      return exec(taskName, null, Collections.emptyMap());
   }

   default CompletionStage<RestResponse> exec(String taskName, Map<String, ?> parameters) {
      return exec(taskName, null, parameters);
   }

}
