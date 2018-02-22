package org.infinispan.persistence.rocksdb.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the RocksDB cache store. For this module, message ids ranging from 23001 to
 * 24000 inclusively have been reserved.
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   @LogMessage(level = ERROR)
   @Message(value = "Error executing parallel store task", id = 252)
   void errorExecutingParallelStoreTask(@Cause Throwable cause);

   @LogMessage(level = INFO)
   @Message(value = "Ignoring XML attribute %s, please remove from configuration file", id = 293)
   void ignoreXmlAttribute(Object attribute);
}
