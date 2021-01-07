package org.infinispan.notifications.cachemanagerlistener.event;

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

   EventType getConfigurationEventType();

   String getConfigurationEntityType();

   String getConfigurationEntityName();
}
