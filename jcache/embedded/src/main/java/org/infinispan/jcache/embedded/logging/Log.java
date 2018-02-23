package org.infinispan.jcache.embedded.logging;

import static org.jboss.logging.Logger.Level.ERROR;

import java.util.Collection;

import org.infinispan.lifecycle.ComponentStatus;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   @Message(value = "'%s' parameter must not be null", id = 21010)
   NullPointerException parameterMustNotBeNull(String parameterName);

   @LogMessage(level = ERROR)
   @Message(value = "Error loading %s keys from persistence store", id = 21017)
   void errorLoadingAll(Collection<?> keysToLoad, @Cause Throwable t);

   @Message(value = "Cache manager is in %s status", id = 21020)
   IllegalStateException cacheManagerClosed(ComponentStatus status);

   @Message(value = "Cache is in %s status", id = 21021)
   IllegalStateException cacheClosed(ComponentStatus status);
}
