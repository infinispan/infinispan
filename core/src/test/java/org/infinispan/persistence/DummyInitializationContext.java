package org.infinispan.persistence;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class DummyInitializationContext implements InitializationContext {
   StoreConfiguration clc;
   Cache cache;
   PersistenceMarshaller marshaller;

   ByteBufferFactory byteBufferFactory;
   MarshallableEntryFactory marshalledEntryFactory;
   ExecutorService executorService;

   GlobalConfiguration globalConfiguration;
   BlockingManager manager;
   TimeService timeService;

   public DummyInitializationContext() {
   }

   public DummyInitializationContext(StoreConfiguration clc, Cache cache, PersistenceMarshaller marshaller,
                                     ByteBufferFactory byteBufferFactory, MarshallableEntryFactory marshalledEntryFactory,
                                     ExecutorService executorService, GlobalConfiguration globalConfiguration,
                                     BlockingManager manager, TimeService timeService) {
      this.clc = clc;
      this.cache = cache;
      this.marshaller = marshaller;
      this.byteBufferFactory = byteBufferFactory;
      this.marshalledEntryFactory = marshalledEntryFactory;
      this.executorService = executorService;
      this.globalConfiguration = globalConfiguration;
      this.manager = manager;
      this.timeService = timeService;
   }

   @Override
   public StoreConfiguration getConfiguration() {
      return clc;
   }

   @Override
   public Cache getCache() {
      return cache;
   }

   @Override
   public KeyPartitioner getKeyPartitioner() {
      return cache.getAdvancedCache().getComponentRegistry().getComponent(KeyPartitioner.class);
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
      return marshalledEntryFactory;
   }

   @Override
   public ExecutorService getExecutor() {
      return executorService;
   }

   @Override
   public Executor getNonBlockingExecutor() {
      return executorService;
   }

   @Override
   public BlockingManager getBlockingManager() {
      return manager;
   }

   @Override
   public PersistenceMarshaller getPersistenceMarshaller() {
      return marshaller;
   }

   @Override
   public GlobalConfiguration getGlobalConfiguration() {
      return globalConfiguration;
   }
}
