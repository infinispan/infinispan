package org.infinispan.server.core;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.encoding.impl.TwoStepTranscoder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.server.core.dataconversion.JsonTranscoder;
import org.infinispan.server.core.dataconversion.XMLTranscoder;

/**
 * Server module lifecycle callbacks
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@InfinispanModule(name = "server-core", requiredModules = "core", optionalModules = "jboss-marshalling")
public class LifecycleCallbacks implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, new PersistenceContextInitializerImpl());

      ClassAllowList classAllowList = gcr.getComponent(EmbeddedCacheManager.class).getClassAllowList();
      ClassLoader classLoader = globalConfiguration.classLoader();

      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      JsonTranscoder jsonTranscoder = new JsonTranscoder(classLoader, classAllowList);

      encoderRegistry.registerTranscoder(jsonTranscoder);
      registerXmlTranscoder(encoderRegistry, classLoader, classAllowList);

      // Allow transcoding between JBoss Marshalling and JSON
      if (encoderRegistry.isConversionSupported(MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_JBOSS_MARSHALLING)) {
         Transcoder jbossMarshallingTranscoder =
               encoderRegistry.getTranscoder(MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_JBOSS_MARSHALLING);
         encoderRegistry.registerTranscoder(new TwoStepTranscoder(jbossMarshallingTranscoder, jsonTranscoder));
      }
   }

   // This method is here for Quarkus to replace. If this method is moved or modified Infinispan Quarkus will also
   // be required to be updated
   private void registerXmlTranscoder(EncoderRegistry encoderRegistry, ClassLoader classLoader, ClassAllowList classAllowList) {
      encoderRegistry.registerTranscoder(new XMLTranscoder(classLoader, classAllowList));
   }
}
