package org.infinispan.persistence;

import java.util.concurrent.ExecutorService;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
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
   private final MarshalledEntryFactory marshalledEntryFactory;
   private final MarshallableEntryFactory marshallableEntryFactory;
   private final ExecutorService executorService;
   private final GlobalConfiguration globalConfiguration;


   public InitializationContextImpl(StoreConfiguration configuration, Cache cache, KeyPartitioner keyPartitioner,
                                    PersistenceMarshaller marshaller, TimeService timeService,
                                    ByteBufferFactory byteBufferFactory, MarshalledEntryFactory marshalledEntryFactory,
                                    MarshallableEntryFactory marshallableEntryFactory, ExecutorService executorService,
                                    GlobalConfiguration globalConfiguration) {
      this.configuration = configuration;
      this.cache = cache;
      this.keyPartitioner = keyPartitioner;
      this.marshaller = marshaller;
      this.timeService = timeService;
      this.byteBufferFactory = byteBufferFactory;
      this.marshalledEntryFactory = marshalledEntryFactory;
      this.marshallableEntryFactory = marshallableEntryFactory;
      this.executorService = executorService;
      this.globalConfiguration = globalConfiguration;
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
      return new StreamingMarshallerBridge(marshaller);
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
   public ExecutorService getExecutor() {
      return executorService;
   }

   @Override
   public MarshalledEntryFactory getMarshalledEntryFactory() {
      return marshalledEntryFactory;
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
