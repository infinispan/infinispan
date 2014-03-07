package org.infinispan.persistence.leveldb.logging;

import org.infinispan.persistence.spi.PersistenceException;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the LevelDB cache store. For this module, message ids ranging from 23001 to
 * 24000 inclusively have been reserved.
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

   @LogMessage(level = DEBUG)
   @Message(value = "unable to instantiate DB Factory: %s", id = 23004)
   void debugUnableToInstantiateDbFactory(String className, @Cause Throwable throwable);

   @LogMessage(level = INFO)
   @Message(value = "Using JNI LevelDB implementation: %s", id = 23005)
   void infoUsingJNIDbFactory(String className);

   @LogMessage(level = INFO)
   @Message(value = "Using pure Java LevelDB implementation: %s", id = 23006)
   void infoUsingJavaDbFactory(String className);

   @Message(value = "Could not load any LevelDB Factories: : %s", id = 23007)
   PersistenceException cannotLoadlevelDBFactories(String formattedArrayOfClassNames);
}
