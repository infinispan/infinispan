package org.infinispan.cloudevents;

import static org.infinispan.commons.logging.Log.CONFIG;

import org.infinispan.cloudevents.configuration.CloudEventsConfiguration;
import org.infinispan.cloudevents.configuration.CloudEventsGlobalConfiguration;
import org.infinispan.cloudevents.impl.EntryEventListener;
import org.infinispan.cloudevents.impl.KafkaEventSender;
import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.DynamicModuleMetadataProvider;
import org.infinispan.factories.impl.ModuleMetadataBuilder;
import org.infinispan.lifecycle.ModuleLifecycle;

/**
 * Install the required components for CloudEvents integration
 *
 * @author Dan Berindei
 * @since 12
 */
@Experimental
@InfinispanModule(name = "cloudevents", requiredModules = "core")
public final class CloudEventsModule implements ModuleLifecycle, DynamicModuleMetadataProvider {

   public static final String CLOUDEVENTS_FEATURE = "cloudevents";

   private GlobalConfiguration globalConfiguration;
   private CloudEventsGlobalConfiguration cloudEventsGlobalConfiguration;

   @Override
   public void registerDynamicMetadata(ModuleMetadataBuilder.ModuleBuilder moduleBuilder, GlobalConfiguration globalConfiguration) {
   }

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      this.globalConfiguration = globalConfiguration;

      cloudEventsGlobalConfiguration = globalConfiguration.module(CloudEventsGlobalConfiguration.class);
      if (cloudEventsGlobalConfiguration == null)
         return;

      if (!cloudEventsGlobalConfiguration.cacheEntryEventsEnabled() && !cloudEventsGlobalConfiguration.auditEventsEnabled())
         return;

      if (!globalConfiguration.features().isAvailable(CLOUDEVENTS_FEATURE))
         throw CONFIG.featureDisabled(CLOUDEVENTS_FEATURE);

      // Eagerly create and register the sender component
      gcr.getComponent(KafkaEventSender.class);
   }

   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {
      if (!cloudEventsGlobalConfiguration.cacheEntryEventsEnabled())
         return;

      CloudEventsConfiguration cloudEventsConfiguration = configuration.module(CloudEventsConfiguration.class);
      if (cloudEventsConfiguration != null && !cloudEventsConfiguration.enabled())
         return;

      if (!globalConfiguration.features().isAvailable(CLOUDEVENTS_FEATURE))
         throw CONFIG.featureDisabled(CLOUDEVENTS_FEATURE);

      EntryEventListener listener = new EntryEventListener();
      cr.registerComponent(listener, EntryEventListener.class);
   }
}
