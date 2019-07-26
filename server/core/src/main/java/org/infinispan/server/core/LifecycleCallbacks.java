package org.infinispan.server.core;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.dataconversion.BinaryTranscoder;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.server.core.dataconversion.JBossMarshallingTranscoder;
import org.infinispan.server.core.dataconversion.JavaSerializationTranscoder;
import org.infinispan.server.core.dataconversion.JsonTranscoder;
import org.infinispan.server.core.dataconversion.ProtostreamBinaryTranscoder;
import org.infinispan.server.core.dataconversion.XMLTranscoder;

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.commons.marshall.AdvancedExternalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@InfinispanModule(name = "server-core", requiredModules = "core")
public class LifecycleCallbacks implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      ClassWhiteList classWhiteList = gcr.getComponent(EmbeddedCacheManager.class).getClassWhiteList();
      ClassLoader classLoader = globalConfiguration.classLoader();
      GenericJBossMarshaller marshaller = new GenericJBossMarshaller(classLoader, classWhiteList);

      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      JsonTranscoder jsonTranscoder = new JsonTranscoder(classLoader, classWhiteList);

      encoderRegistry.registerTranscoder(jsonTranscoder);
      encoderRegistry.registerTranscoder(new JBossMarshallingTranscoder(jsonTranscoder, marshaller));
      encoderRegistry.registerTranscoder(new XMLTranscoder(classLoader, classWhiteList));
      encoderRegistry.registerTranscoder(new JavaSerializationTranscoder(classWhiteList));
      encoderRegistry.registerTranscoder(new ProtostreamBinaryTranscoder());

      BinaryTranscoder transcoder = encoderRegistry.getTranscoder(BinaryTranscoder.class);
      transcoder.overrideMarshaller(marshaller);
   }
}
