package org.infinispan.server.core;

import java.util.EnumSet;
import java.util.concurrent.ExecutorService;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.dataconversion.BinaryTranscoder;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.server.core.dataconversion.JBossMarshallingTranscoder;
import org.infinispan.server.core.dataconversion.JavaSerializationTranscoder;
import org.infinispan.server.core.dataconversion.JsonTranscoder;
import org.infinispan.server.core.dataconversion.XMLTranscoder;
import org.infinispan.server.core.transport.NettyTransport;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

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
      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, new PersistenceContextInitializerImpl());

      BasicComponentRegistry basicComponentRegistry = gcr.getComponent(BasicComponentRegistry.class);
      InternalCacheRegistry cacheRegistry = basicComponentRegistry.getComponent(InternalCacheRegistry.class).running();
      cacheRegistry.registerInternalCache(SERVER_STATE_CACHE, getServerStateCacheConfig(globalConfiguration).build(),
            EnumSet.of(InternalCacheRegistry.Flag.PERSISTENT));

      ClassWhiteList classWhiteList = gcr.getComponent(EmbeddedCacheManager.class).getClassWhiteList();
      ClassLoader classLoader = globalConfiguration.classLoader();
      Marshaller jbossMarshaller = Util.getJBossMarshaller(classLoader, classWhiteList);

      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      JsonTranscoder jsonTranscoder = new JsonTranscoder(classLoader, classWhiteList);

      encoderRegistry.registerTranscoder(jsonTranscoder);
      encoderRegistry.registerTranscoder(new XMLTranscoder(classLoader, classWhiteList));
      encoderRegistry.registerTranscoder(new JavaSerializationTranscoder(classWhiteList));

      if (jbossMarshaller != null) {
         encoderRegistry.registerTranscoder(new JBossMarshallingTranscoder(jsonTranscoder, jbossMarshaller));
         BinaryTranscoder transcoder = encoderRegistry.getTranscoder(BinaryTranscoder.class);
         transcoder.overrideMarshaller(jbossMarshaller);
      }

      ComponentRef<ExecutorService> executorServiceRef = basicComponentRegistry.getComponent(
            KnownComponentNames.NON_BLOCKING_EXECUTOR, ExecutorService.class);
      ExecutorService oldNonBlockingExecutor = executorServiceRef.wired();

      String nodeName = globalConfiguration.transport().nodeName();

      EventLoopGroup eventLoopGroup = NettyTransport.buildEventLoop(Runtime.getRuntime().availableProcessors(),
            new DefaultThreadFactory("non-blocking-thread-netty" + (nodeName != null ? "-" + nodeName : "")));

      basicComponentRegistry.replaceComponent(KnownComponentNames.NON_BLOCKING_EXECUTOR, eventLoopGroup, true);
      basicComponentRegistry.rewire();
      // TODO: need to ask if this is required or not - I don't think so
      gcr.rewire();
      basicComponentRegistry.registerComponent(EventLoopGroup.class, eventLoopGroup, false);

      oldNonBlockingExecutor.shutdown();
   }

   private ConfigurationBuilder getServerStateCacheConfig(GlobalConfiguration globalConfiguration) {
      CacheMode cacheMode = globalConfiguration.isClustered() ? CacheMode.REPL_SYNC : CacheMode.LOCAL;
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(cacheMode);
      return builder;
   }
}
