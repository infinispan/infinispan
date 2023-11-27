package org.infinispan.jcache.remote.logging;

import static org.jboss.logging.Logger.Level.WARN;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * @author gustavonalle
 * @since 8.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

   @LogMessage(level = WARN)
   @Message(value = "Timeout waiting event notification for cache operation.", id = 21029)
   void timeoutWaitingEvent();

//   @Message(value = "Creating a cache not supported without management access.", id = 21050)
//   UnsupportedOperationException createCacheNotAllowedWithoutManagement();

//   @Message(value = "Removing a cache not supported without management access.", id = 21051)
//   UnsupportedOperationException removeCacheNotAllowedWithoutManagement();

//   @Message(value = "Cache '%s' has been already been predefined.", id = 21052)
//   CacheException cacheNamePredefined(String cacheName);

}
