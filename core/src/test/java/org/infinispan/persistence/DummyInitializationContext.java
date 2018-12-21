package org.infinispan.persistence;

import java.util.concurrent.ExecutorService;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntryFactory;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class DummyInitializationContext implements InitializationContext {
   StoreConfiguration clc;
   Cache cache;
   StreamingMarshaller marshaller;

   ByteBufferFactory byteBufferFactory;
   MarshallableEntryFactory marshalledEntryFactory;
   ExecutorService executorService;

   public DummyInitializationContext() {
   }

   public DummyInitializationContext(StoreConfiguration clc, Cache cache, StreamingMarshaller marshaller,
                                     ByteBufferFactory byteBufferFactory, MarshallableEntryFactory marshalledEntryFactory,
                                     ExecutorService executorService) {
      this.clc = clc;
      this.cache = cache;
      this.marshaller = marshaller;
      this.byteBufferFactory = byteBufferFactory;
      this.marshalledEntryFactory = marshalledEntryFactory;
      this.executorService = executorService;
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
   public StreamingMarshaller getMarshaller() {
      return marshaller;
   }

   @Override
   public TimeService getTimeService() {
      return cache.getAdvancedCache().getComponentRegistry().getTimeService();
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
   public MarshalledEntryFactory getMarshalledEntryFactory() {
      throw new UnsupportedOperationException("Use InitializationContext::getMarshallableEntryFactory instead");
   }
}
