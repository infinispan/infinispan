package org.infinispan.server.core;

import java.util.EnumSet;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.encoding.impl.TwoStepTranscoder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.registry.InternalCacheRegistry;
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

   static final String SERVER_STATE_CACHE = "org.infinispan.SERVER_STATE";

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, new PersistenceContextInitializerImpl());

      BasicComponentRegistry basicComponentRegistry = gcr.getComponent(BasicComponentRegistry.class);
      InternalCacheRegistry cacheRegistry = basicComponentRegistry.getComponent(InternalCacheRegistry.class).running();
      cacheRegistry.registerInternalCache(SERVER_STATE_CACHE, getServerStateCacheConfig(globalConfiguration).build(),
            EnumSet.of(InternalCacheRegistry.Flag.PERSISTENT));

      ClassAllowList classAllowList = gcr.getComponent(EmbeddedCacheManager.class).getClassAllowList();
      ClassLoader classLoader = globalConfiguration.classLoader();

      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      JsonTranscoder jsonTranscoder = new JsonTranscoder(classLoader, classAllowList);

      encoderRegistry.registerTranscoder(jsonTranscoder);
      encoderRegistry.registerTranscoder(new XMLTranscoder(classLoader, classAllowList));

      // Allow transcoding between JBoss Marshalling and JSON
      if (encoderRegistry.isConversionSupported(MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_JBOSS_MARSHALLING)) {
         Transcoder jbossMarshallingTranscoder =
               encoderRegistry.getTranscoder(MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_JBOSS_MARSHALLING);
         encoderRegistry.registerTranscoder(new TwoStepTranscoder(jbossMarshallingTranscoder, jsonTranscoder));
      }
   }

   private ConfigurationBuilder getServerStateCacheConfig(GlobalConfiguration globalConfiguration) {
      CacheMode cacheMode = globalConfiguration.isClustered() ? CacheMode.REPL_SYNC : CacheMode.LOCAL;
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(cacheMode);
      return builder;
   }
}
