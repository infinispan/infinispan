package org.infinispan.loaders.cassandra.logging;

import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import static org.jboss.logging.Logger.Level.ERROR;

/**
 * Log abstraction for the cassandra store. For this module, message ids
 * ranging from 3001 to 4000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = ERROR)
   @Message(value = "Error removing key %s", id = 3001)
   void errorRemovingKey(Object key, @Cause Exception e);

}
