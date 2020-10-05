package org.infinispan.cloudevents.impl;

import static org.jboss.logging.Logger.Level.ERROR;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.notifications.cachelistener.event.Event;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * This module reserves range 30501 - 31000
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   Log CONFIG = Logger.getMessageLogger(Log.class, org.infinispan.util.logging.Log.LOG_ROOT + "CONFIG");

   @LogMessage(level = ERROR)
   @Message(value = "Failed to send cloudevents message for %s %s %s event", id = 30501)
   void sendError(Event.Type eventType, Object key, Object source);

   @Message(value = "Attribute 'bootstrap-servers' is required when a topic is enabled")
   CacheConfigurationException bootstrapServersRequired();
}
