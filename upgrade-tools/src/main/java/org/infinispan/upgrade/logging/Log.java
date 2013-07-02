package org.infinispan.upgrade.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.WARN;

import org.infinispan.commons.CacheException;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Cause;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

/**
 * Log abstraction for the Rolling Upgrade Tools. For this module, message ids
 * ranging from 20001 to 21000 inclusively have been reserved.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {
   @LogMessage(level = ERROR)
   @Message(value = "Could not register upgrade MBean", id = 20001)
   void jmxRegistrationFailed();

   @LogMessage(level = ERROR)
   @Message(value = "Could not unregister upgrade MBean", id = 20002)
   void jmxUnregistrationFailed();

   @Message(value = "The RemoteCacheStore for cache %s should be configured with hotRodWrapping enabled", id = 20003)
   CacheException remoteStoreNoHotRodWrapping(String cacheName);

   @Message(value = "Could not find migration data in cache %s", id = 20004)
   CacheException missingMigrationData(String name);

   @LogMessage(level = WARN)
   @Message(value = "Could not migrate key %s", id = 20005)
   void keyMigrationFailed(String key, @Cause Throwable cause);
}
