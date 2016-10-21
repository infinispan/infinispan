package org.infinispan.persistence.rocksdb.logging;

import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import static org.jboss.logging.Logger.Level.DEBUG;
import static org.jboss.logging.Logger.Level.WARN;

/**
 * Log abstraction for the RocksDB cache store. For this module, message ids ranging from 23001 to
 * 24000 inclusively have been reserved.
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = WARN)
   @Message(value = "unable to close iterator", id = 23001)
   void warnUnableToCloseDbIterator(@Cause Throwable throwable);

   @LogMessage(level = WARN)
   @Message(value = "unable to close db", id = 23002)
   void warnUnableToCloseDb(@Cause Throwable throwable);

   @LogMessage(level = WARN)
   @Message(value = "unable to close expired db", id = 23003)
   void warnUnableToCloseExpiredDb(@Cause Throwable throwable);

   @LogMessage(level = DEBUG)
   @Message(value = "An internal RocksDB exception occurred", id = 23008)
   void warnAboutExceptionInRocksDB(@Cause Exception e);
}
