package org.infinispan.persistence.spi;

import java.util.concurrent.ExecutorService;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.StreamAwareMarshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;

import net.jcip.annotations.ThreadSafe;

/**
 * Aggregates the initialisation state needed by either a {@link CacheLoader} or a {@link CacheWriter}.
 *
 * @author Mircea Markus
 * @since 6.0
 */
@ThreadSafe
public interface InitializationContext {

   <T extends StoreConfiguration> T getConfiguration();

   Cache getCache();

   /**
    * The configured partitioner that can be used to determine which segment a given key belongs to. This is useful
    * when a store is segmented (ie. implements {@link SegmentedAdvancedLoadWriteStore}).
    * @return partitioner that can provide what segment a key maps to
    */
   KeyPartitioner getKeyPartitioner();

   /**
    * Returns a wrapped version of {@link #getPersistenceMarshaller()}, which will throw a {@link UnsupportedOperationException}
    * for any of the operations requiring {@link java.io.ObjectOutput} or {@link java.io.ObjectInput}.
    *
    * @deprecated use {@link #getPersistenceMarshaller()} instead
    */
   @Deprecated
   StreamingMarshaller getMarshaller();

   TimeService getTimeService();

   /**
    * To be used for building {@link org.infinispan.commons.io.ByteBuffer} objects.
    */
   ByteBufferFactory getByteBufferFactory();

   /**
    * To be used for building {@link MarshalledEntry} objects.
    */
   MarshalledEntryFactory getMarshalledEntryFactory();

   /**
    * Returns the preferred executor to be used by stores if needed. Stores normally shouldn't need this unless they
    * *must* perform some blocking code asynchronously.
    * @return the executor to be used with stores
    */
   ExecutorService getExecutor();

   /**
    * Returns the persistence marshaller which should be used to marshall/unmarshall all stored bytes.
    */
   StreamAwareMarshaller getPersistenceMarshaller();
}
