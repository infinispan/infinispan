package org.infinispan.jboss.marshalling;

import static org.infinispan.util.logging.Log.PERSISTENCE;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.jboss.marshalling.core.JBossUserMarshaller;
import org.infinispan.jboss.marshalling.dataconversion.JBossMarshallingTranscoder;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;

/**
 * JBoss Marshalling module lifecycle callbacks
 *
 * <p>Registers a JBoss Marshalling encoder and transcoder.</p>
 *
 * @author Dan Berindei
 * @since 11.0
 */
@InfinispanModule(name = "jboss-marshalling", requiredModules = "core")
public class JbossMarshallingModule implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      PERSISTENCE.jbossMarshallingDetected();

      Marshaller userMarshaller = globalConfiguration.serialization().marshaller();
      if (userMarshaller instanceof JBossUserMarshaller) {
         // Core automatically registers a transcoder for the user marshaller
         // Initialize the externalizers from the serialization configuration
         ((JBossUserMarshaller) userMarshaller).initialize(gcr);
      } else {
         // Register a JBoss Marshalling transcoder, ignoring any configured externalizers
         ClassAllowList classAllowList = gcr.getComponent(EmbeddedCacheManager.class).getClassAllowList();
         ClassLoader classLoader = globalConfiguration.classLoader();
         GenericJBossMarshaller jbossMarshaller = new GenericJBossMarshaller(classLoader, classAllowList);
         EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
         encoderRegistry.registerTranscoder(new JBossMarshallingTranscoder(jbossMarshaller));
      }
   }
}
