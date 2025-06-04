package org.infinispan.persistence;

import java.util.concurrent.Executor;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.NonBlockingManager;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class InitializationContextImpl implements InitializationContext {

   private final StoreConfiguration configuration;
   private final Cache cache;
   private final KeyPartitioner keyPartitioner;
   private final PersistenceMarshaller marshaller;
   private final TimeService timeService;
   private final ByteBufferFactory byteBufferFactory;
   private final MarshallableEntryFactory marshallableEntryFactory;
   private final Executor nonBlockingExecutor;
   private final GlobalConfiguration globalConfiguration;
   private final BlockingManager blockingManager;
   private final NonBlockingManager nonBlockingManager;


   public InitializationContextImpl(StoreConfiguration configuration, Cache cache, KeyPartitioner keyPartitioner,
                                    PersistenceMarshaller marshaller, TimeService timeService,
                                    ByteBufferFactory byteBufferFactory, MarshallableEntryFactory marshallableEntryFactory,
                                    Executor nonBlockingExecutor, GlobalConfiguration globalConfiguration,
                                    BlockingManager blockingManager, NonBlockingManager nonBlockingManager) {
      this.configuration = configuration;
      this.cache = cache;
      this.keyPartitioner = keyPartitioner;
      this.marshaller = marshaller;
      this.timeService = timeService;
      this.byteBufferFactory = byteBufferFactory;
      this.marshallableEntryFactory = marshallableEntryFactory;
      this.nonBlockingExecutor = nonBlockingExecutor;
      this.globalConfiguration = globalConfiguration;
      this.blockingManager = blockingManager;
      this.nonBlockingManager = nonBlockingManager;
   }

   @Override
   public StoreConfiguration getConfiguration() {
      return configuration;
   }

   @Override
   public Cache getCache() {
      return cache;
   }

   @Override
   public KeyPartitioner getKeyPartitioner() {
      return keyPartitioner;
   }

   @Override
   public TimeService getTimeService() {
      return timeService;
   }

   @Override
   public ByteBufferFactory getByteBufferFactory() {
      return byteBufferFactory;
   }

   @Override
   public <K,V> MarshallableEntryFactory<K,V> getMarshallableEntryFactory() {
      //noinspection unchecked
      return marshallableEntryFactory;
   }

   @Override
   public Executor getNonBlockingExecutor() {
      return nonBlockingExecutor;
   }

   @Override
   public BlockingManager getBlockingManager() {
      return blockingManager;
   }

   @Override
   public NonBlockingManager getNonBlockingManager() {
      return nonBlockingManager;
   }

   @Override
   public PersistenceMarshaller getPersistenceMarshaller() {
      return marshaller;
   }

   @Override
   public GlobalConfiguration getGlobalConfiguration() {
      return globalConfiguration;
   }

   @Override
   public boolean canStoreDirectlyPurgeOnStartup() {
      // purgeOnStartup is false, so no we can't
      if (!InitializationContext.super.canStoreDirectlyPurgeOnStartup()) {
         return false;
      }

      GlobalStateManager gsm = ComponentRegistry.of(getCache())
            .getGlobalComponentRegistry()
            .getComponent(GlobalStateManager.class);

      // If the global state is present that means we can't purge if this cache is part of state transfer as its
      // contents may be required to send to other nodes during state transfer before clearing
      return gsm.readScopedState(getCache().getName()).isEmpty() ||
            !getCache().getCacheConfiguration().clustering().cacheMode().needsStateTransfer();
   }
}
