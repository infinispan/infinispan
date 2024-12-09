package org.infinispan.rest.listener;

import java.io.StringWriter;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ConfigurationChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ConfigurationChangedEvent;
import org.infinispan.rest.EventStream;
import org.infinispan.rest.ServerSentEvent;
import org.infinispan.security.Security;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.util.logging.annotation.impl.Logged;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogSerializer;

@Listener
public class ServerEventsListener implements Runnable {

   private final EventStream eventStream;
   private final MediaType mediaType;
   private final boolean pretty;
   private final ParserRegistry parserRegistry;
   private final EmbeddedCacheManager cacheManager;
   private final Set<EventLogCategory> acceptedCategories;

   public ServerEventsListener(MediaType mediaType, boolean pretty, ParserRegistry parserRegistry, EmbeddedCacheManager cacheManager, Set<EventLogCategory> acceptedCategories, boolean includeState) {
      this.mediaType = mediaType;
      this.pretty = pretty;
      this.parserRegistry = parserRegistry;
      this.cacheManager = cacheManager;
      this.acceptedCategories = acceptedCategories;
      this.eventStream = includeState ?
            new EventStream(this::sendAllConfiguration, this) :
            new EventStream(null, this);
   }

   public EventStream getEventStream() {
      return eventStream;
   }

   public CompletionStage<Void> registerConfigurationEvents() {
      return SecurityActions.addListenerAsync(cacheManager, this);
   }

   public CompletionStage<Void> registerLogEvents() {
      return SecurityActions.addLoggerListenerAsync(cacheManager, this);
   }

   @Override
   public void run() {
      //on event stream closed
      Security.doPrivileged(() -> cacheManager.removeListenerAsync(this));
   }

   @ConfigurationChanged
   public CompletionStage<Void> onConfigurationEvent(ConfigurationChangedEvent event) {
      String eventType = event.getConfigurationEventType().toString().toLowerCase() + "-" + event.getConfigurationEntityType();
      if (event.getConfigurationEventType() == ConfigurationChangedEvent.EventType.REMOVE) {
         return send(eventType, event.getConfigurationEntityName());
      }
      return switch (event.getConfigurationEntityType()) {
         case "cache", "template" -> {
            Configuration config = SecurityActions.getCacheConfiguration(cacheManager, event.getConfigurationEntityName());
            yield send(eventType, serializeConfiguration(config, event.getConfigurationEntityName()));
         }
         // Unhandled entity type, ignore
         default -> CompletableFutures.completedNull();
      };
   }

   @Logged
   public CompletionStage<Void> onDataLogged(EventLog event) {
      if (!acceptedCategories.contains(event.getCategory())) {
         return CompletableFutures.completedNull();
      }
      var eventType = event.getCategory().toString().toLowerCase(Locale.ROOT).replace('_', '-') + "-event";
      return send(eventType, serializeEvent(event));
   }

   private CompletionStage<Void> send(String type, String data) {
      return eventStream.sendEvent(new ServerSentEvent(type, data));
   }

   private String serializeConfiguration(Configuration cacheConfiguration, String name) {
      StringWriter sw = new StringWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).withType(mediaType).prettyPrint(pretty).build()) {
         parserRegistry.serialize(writer, null, Collections.singletonMap(name, cacheConfiguration));
      }
      return sw.toString();
   }

   private String serializeEvent(EventLog event) {
      StringWriter sw = new StringWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).withType(mediaType).prettyPrint(pretty).build()) {
         parserRegistry.serializeWith(writer, new EventLogSerializer(), event);
      }
      return sw.toString();
   }

   private void sendAllConfiguration(EventStream ignored) {
      for (String configName : cacheManager.getCacheConfigurationNames()) {
         Configuration config = SecurityActions.getCacheConfiguration(cacheManager, configName);
         String eventType = config.isTemplate() ? "create-template" : "create-cache";
         send(eventType, serializeConfiguration(config, configName));
      }
   }
}
