package org.infinispan.persistence.support;

import java.util.concurrent.Executor;

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
import org.infinispan.util.concurrent.NonBlockingManager;

public abstract class DelegatingInitializationContext implements InitializationContext {
   public abstract InitializationContext delegate();

   @Override
   public <T extends StoreConfiguration> T getConfiguration() {
      return delegate().getConfiguration();
   }

   @Override
   public Cache getCache() {
      return delegate().getCache();
   }

   @Override
   public KeyPartitioner getKeyPartitioner() {
      return delegate().getKeyPartitioner();
   }

   @Override
   public TimeService getTimeService() {
      return delegate().getTimeService();
   }

   @Override
   public ByteBufferFactory getByteBufferFactory() {
      return delegate().getByteBufferFactory();
   }

   @Override
   public Executor getNonBlockingExecutor() {
      return delegate().getNonBlockingExecutor();
   }

   @Override
   public BlockingManager getBlockingManager() {
      return delegate().getBlockingManager();
   }

   @Override
   public NonBlockingManager getNonBlockingManager() {
      return delegate().getNonBlockingManager();
   }

   @Override
   public <K, V> MarshallableEntryFactory<K, V> getMarshallableEntryFactory() {
      return delegate().getMarshallableEntryFactory();
   }

   @Override
   public PersistenceMarshaller getPersistenceMarshaller() {
      return delegate().getPersistenceMarshaller();
   }

   @Override
   public GlobalConfiguration getGlobalConfiguration() {
      return delegate().getGlobalConfiguration();
   }
}
