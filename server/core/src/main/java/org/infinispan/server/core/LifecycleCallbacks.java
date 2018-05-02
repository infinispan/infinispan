package org.infinispan.server.core;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.server.core.dataconversion.JBossMarshallingTranscoder;
import org.infinispan.server.core.dataconversion.JavaSerializationTranscoder;
import org.infinispan.server.core.dataconversion.JsonTranscoder;
import org.infinispan.server.core.dataconversion.XMLTranscoder;

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.commons.marshall.AdvancedExternalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class LifecycleCallbacks implements ModuleLifecycle {

   static ComponentMetadataRepo componentMetadataRepo;

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      componentMetadataRepo = gcr.getComponentMetadataRepo();
      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      encoderRegistry.registerTranscoder(new JsonTranscoder());
      encoderRegistry.registerTranscoder(new JBossMarshallingTranscoder(encoderRegistry));
      encoderRegistry.registerTranscoder(new XMLTranscoder());
      encoderRegistry.registerTranscoder(new JavaSerializationTranscoder());

   }
}
