package org.infinispan.notifications.cachemanagerlistener.event;

import java.util.Map;

/**
 * This event is passed in to any method annotated with {@link org.infinispan.notifications.cachemanagerlistener.annotation.ConfigurationChanged}.
 *
 * @author Tristan Tarrant
 * @since 13.0
 */
public interface ConfigurationChangedEvent extends Event {

   enum EventType {
      CREATE,
      UPDATE,
      REMOVE
   }

   String CACHE = "cache";
   String TEMPLATE = "template";
   String SCHEMA = "schema";

   EventType getConfigurationEventType();

   String getConfigurationEntityType();

   String getConfigurationEntityName();

   Map<String, Object> getConfigurationEntityValue();
}
