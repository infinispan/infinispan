package org.infinispan.tasks.manager.logging;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Log abstraction for the Tasks module. For this module, message ids ranging from 27001 to
 * 27500 inclusively have been reserved.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 27001, max = 27500)
public interface Log extends BasicLogger {
   /*@Message(value = "Task Engine '%s' has already been registered", id = 27001)
   IllegalStateException duplicateTaskEngineRegistration(String taskEngineName);*/

   @Message(value = "Unknown task '%s'", id = 27002)
   IllegalArgumentException unknownTask(String taskName);


}
