package org.infinispan.loaders.bdbje.logging;

import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the bdbje cache store. For this module, message ids
 * ranging from 2001 to 3000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = ERROR)
   @Message(value = "Error closing database", id = 2001)
   void errorClosingDatabase(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Error closing catalog", id = 2002)
   void errorClosingCatalog(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Error rolling back transaction", id = 2003)
   void rollingBackAfterError(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Expected to write %s records, but wrote %s", id = 2004)
   void unexpectedNumberRecordsWritten(long recordCount, int recordWritten);

}
