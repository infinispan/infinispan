package org.infinispan.loaders.leveldb.logging;

import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import static org.jboss.logging.Logger.Level.WARN;

/**
 * Log abstraction for the LevelDB cache store. For this module, message ids ranging from 23001 to 24000 inclusively
 * have been reserved.
 *
 * @author Mircea Markus
 * @since 5.2
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
}
