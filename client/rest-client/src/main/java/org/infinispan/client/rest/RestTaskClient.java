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
    * Retrieves a list of tasks from the server
    */
   CompletionStage<RestResponse> list();

   /**
    * Executes a task with the supplied parameters. Currently only supports String values
    */
   CompletionStage<RestResponse> exec(String taskName, Map<String, ?> parameters);

   /**
    * Uploads a script
    */
   CompletionStage<RestResponse> uploadScript(String taskName, RestEntity script);

   /**
    * Executes a task without parameters
    */
   default CompletionStage<RestResponse> exec(String taskName) {
      return exec(taskName, Collections.emptyMap());
   }

}
