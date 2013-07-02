package org.infinispan.loaders.hbase.logging;

import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import static org.jboss.logging.Logger.Level.ERROR;

/**
 * Log abstraction for the HBase store. For this module, message ids
 * ranging from 18001 to 19000 inclusively have been reserved.
 *
 * @author Justin Hayes
 * @since 5.2
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = ERROR)
   @Message(value = "Error removing key %s", id = 18001)
   void errorRemovingKey(Object key, @Cause Exception e);

}
