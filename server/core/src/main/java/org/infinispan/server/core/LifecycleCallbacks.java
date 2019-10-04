package org.infinispan.server.core;

import java.util.EnumSet;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.dataconversion.BinaryTranscoder;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.registry.InternalCacheRegistry;
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
   static final String SERVER_STATE_CACHE = "org.infinispan.SERVER_STATE";

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      BasicComponentRegistry basicComponentRegistry = gcr.getComponent(BasicComponentRegistry.class);
      InternalCacheRegistry cacheRegistry = basicComponentRegistry.getComponent(InternalCacheRegistry.class).running();
      cacheRegistry.registerInternalCache(SERVER_STATE_CACHE, getServerStateCacheConfig(globalConfiguration).build(),
            EnumSet.of(InternalCacheRegistry.Flag.PERSISTENT));

      ClassWhiteList classWhiteList = gcr.getComponent(EmbeddedCacheManager.class).getClassWhiteList();
      ClassLoader classLoader = globalConfiguration.classLoader();
      Marshaller jbossMarshaller = getJbossMarshaller(classLoader, classWhiteList);

      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      JsonTranscoder jsonTranscoder = new JsonTranscoder(classLoader, classWhiteList);

      encoderRegistry.registerTranscoder(jsonTranscoder);
      encoderRegistry.registerTranscoder(new XMLTranscoder(classLoader, classWhiteList));
      encoderRegistry.registerTranscoder(new JavaSerializationTranscoder(classWhiteList));
      encoderRegistry.registerTranscoder(new ProtostreamBinaryTranscoder());

      if (jbossMarshaller != null) {
         encoderRegistry.registerTranscoder(new JBossMarshallingTranscoder(jsonTranscoder, jbossMarshaller));
         BinaryTranscoder transcoder = encoderRegistry.getTranscoder(BinaryTranscoder.class);
         transcoder.overrideMarshaller(jbossMarshaller);
      }
   }

   private ConfigurationBuilder getServerStateCacheConfig(GlobalConfiguration globalConfiguration) {
      CacheMode cacheMode = globalConfiguration.isClustered() ? CacheMode.REPL_SYNC : CacheMode.LOCAL;
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(cacheMode);
      return builder;
   }

   public static Marshaller getJbossMarshaller(ClassLoader classLoader, ClassWhiteList classWhiteList) {
      try {
         Class<?> marshallerClass = classLoader.loadClass("org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller");
         return Util.newInstanceOrNull(marshallerClass.asSubclass(Marshaller.class),
               new Class[] { ClassLoader.class, ClassWhiteList.class}, classLoader, classWhiteList);
      } catch (ClassNotFoundException e) {
         return null;
      }
   }
}
