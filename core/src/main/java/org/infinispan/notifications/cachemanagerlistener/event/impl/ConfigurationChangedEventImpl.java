package org.infinispan.notifications.cachemanagerlistener.event.impl;

import java.util.Map;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachemanagerlistener.event.ConfigurationChangedEvent;

public class ConfigurationChangedEventImpl implements ConfigurationChangedEvent {

   private final EmbeddedCacheManager cacheManager;
   private final EventType eventType;
   private final String entityType;
   private final String entityName;
   private final Map<String, Object> entityValue;

   public ConfigurationChangedEventImpl(EmbeddedCacheManager cacheManager, EventType eventType, String entityType, String entityName, Map<String, Object> entityValue) {
      this.cacheManager = cacheManager;
      this.eventType = eventType;
      this.entityType = entityType;
      this.entityName = entityName;
      this.entityValue = entityValue;
   }

   @Override
   public EventType getConfigurationEventType() {
      return eventType;
   }

   @Override
   public String getConfigurationEntityType() {
      return entityType;
   }

   @Override
   public String getConfigurationEntityName() {
      return entityName;
   }

   @Override
   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

   @Override
   public Map<String, Object> getConfigurationEntityValue() {
      return entityValue;
   }

   @Override
   public Type getType() {
      return Type.CONFIGURATION_CHANGED;
   }
}
