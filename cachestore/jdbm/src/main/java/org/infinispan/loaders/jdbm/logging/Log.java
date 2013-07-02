package org.infinispan.loaders.jdbm.logging;

import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import java.io.File;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the JDBM cache store. For this module, message ids
 * ranging from 9001 to 10000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = INFO)
   @Message(value = "JDBM database %s opened", id = 9001)
   void jdbmDbOpened(File f);

}
