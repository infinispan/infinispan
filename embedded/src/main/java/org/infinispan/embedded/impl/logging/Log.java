package org.infinispan.embedded.impl.logging;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the embedded module.
 *
 * @since 15.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   String LOG_ROOT = "org.infinispan.";
   Log EMBEDDED = Logger.getMessageLogger(Log.class, LOG_ROOT + "EMBEDDED");

   @Message(value = "Not an Infinispan embedded  URI: %s", id = 13000)
   IllegalArgumentException notAnEmbeddedURI(String string);
}
