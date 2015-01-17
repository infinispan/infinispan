package org.infinispan.jcache.embedded.logging;

import org.infinispan.lifecycle.ComponentStatus;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.jcache.logging.Log {
   @Message(value = "Cache manager is in %s status", id = 21020)
   IllegalStateException cacheManagerClosed(ComponentStatus status);

   @Message(value = "Cache is in %s status", id = 21021)
   IllegalStateException cacheClosed(ComponentStatus status);
}
