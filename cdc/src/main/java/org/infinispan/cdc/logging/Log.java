package org.infinispan.cdc.logging;

import java.lang.invoke.MethodHandles;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * @author José Bolina
 * @since 16.0
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 32501, max = 33000)
public interface Log extends BasicLogger {

   String LOG_ROOT = "org.infinispan.";
   Log CONTAINER = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, LOG_ROOT + "CONTAINER");

   @Message(value = "Invalid configuration for change-data-capture", id = 32501)
   IllegalStateException invalidDatabaseConfiguration(@Cause Throwable cause);
}
