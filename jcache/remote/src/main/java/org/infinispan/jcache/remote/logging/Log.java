package org.infinispan.jcache.remote.logging;

import static org.jboss.logging.Logger.Level.WARN;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * @author gustavonalle
 * @since 8.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.jcache.logging.Log {

   @LogMessage(level = WARN)
   @Message(value = "Timeout waiting event notification for cache operation.", id = 21029)
   void timeoutWaitingEvent();
}
