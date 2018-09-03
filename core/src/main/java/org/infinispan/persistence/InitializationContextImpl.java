package org.infinispan.persistence;

import java.util.concurrent.ExecutorService;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.commons.time.TimeService;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class InitializationContextImpl implements InitializationContext {

   private final StoreConfiguration configuration;
   private final Cache cache;
   private final KeyPartitioner keyPartitioner;
   private final StreamingMarshaller marshaller;
   private final TimeService timeService;
   private final ByteBufferFactory byteBufferFactory;
   private final MarshalledEntryFactory marshalledEntryFactory;
   private final ExecutorService executorService;


   public InitializationContextImpl(StoreConfiguration configuration, Cache cache, KeyPartitioner keyPartitioner,
                                    StreamingMarshaller marshaller, TimeService timeService,
                                    ByteBufferFactory byteBufferFactory, MarshalledEntryFactory mef,
                                    ExecutorService executorService) {
      this.configuration = configuration;
      this.cache = cache;
      this.keyPartitioner = keyPartitioner;
      this.marshaller = marshaller;
      this.timeService = timeService;
      this.byteBufferFactory = byteBufferFactory;
      this.marshalledEntryFactory = mef;
      this.executorService = executorService;
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
   public StreamingMarshaller getMarshaller() {
      return marshaller;
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
   public MarshalledEntryFactory getMarshalledEntryFactory() {
      return marshalledEntryFactory;
   }

   @Override
   public ExecutorService getExecutor() {
      return executorService;
   }
}
