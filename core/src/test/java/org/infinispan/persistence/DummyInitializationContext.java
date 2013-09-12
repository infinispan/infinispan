package org.infinispan.persistence;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.TimeService;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class DummyInitializationContext implements InitializationContext {
   StoreConfiguration clc;
   Cache cache;
   StreamingMarshaller marshaller;

   ByteBufferFactory byteBufferFactory;
   MarshalledEntryFactory marshalledEntryFactory;

   public DummyInitializationContext() {
   }

   public DummyInitializationContext(StoreConfiguration clc, Cache cache, StreamingMarshaller marshaller,
                                     ByteBufferFactory byteBufferFactory, MarshalledEntryFactory marshalledEntryFactory) {
      this.clc = clc;
      this.cache = cache;
      this.marshaller = marshaller;
      this.byteBufferFactory = byteBufferFactory;
      this.marshalledEntryFactory = marshalledEntryFactory;
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
   public MarshalledEntryFactory getMarshalledEntryFactory() {
      return marshalledEntryFactory;
   }
}
