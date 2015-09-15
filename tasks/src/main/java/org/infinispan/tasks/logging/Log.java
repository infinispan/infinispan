package org.infinispan.tasks.logging;

import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the Tasks module. For this module, message ids ranging from 22001 to
 * 23000 inclusively have been reserved.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {
   @Message(value = "Task Engine '%s' has already been registered", id = 22001)
   IllegalStateException duplicateTaskEngineRegistration(String taskEngineName);

   @Message(value = "Unknown task '%s'", id = 22002)
   IllegalArgumentException unknownTask(String taskName);
}
