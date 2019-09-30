package org.infinispan.persistence.spi;

import java.io.ObjectInput;
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
    * Returns a wrapped version of {@link #getPersistenceMarshaller()}, which delegates all {@link java.io.ObjectOutput}
    * and {@link java.io.ObjectInput} calls to the underlying marshaller. Note, calls to {@link ObjectInput#readLine()}
    * on the returned {@link ObjectInput} instance will throw a {@link UnsupportedOperationException}.
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
    * To be used for building {@link org.infinispan.marshall.core.MarshalledEntry} objects.
    * @deprecated since 10.0 please use {@link #getMarshallableEntryFactory()} instead
    */
   @Deprecated
   MarshalledEntryFactory getMarshalledEntryFactory();

   /**
    * Returns the preferred executor to be used by stores if needed. Stores normally shouldn't need this unless they
    * *must* perform some blocking code asynchronously.
    * @return the executor to be used with stores
    */
   ExecutorService getExecutor();

   /**
    * Should be used to build all {@link MarshallableEntry} objects.
    */
   <K,V> MarshallableEntryFactory<K,V> getMarshallableEntryFactory();

   /**
    * Returns the persistence marshaller which should be used to marshall/unmarshall all stored bytes.
    */
   PersistenceMarshaller getPersistenceMarshaller();

   /**
    * Returns the global configuration
    */
   GlobalConfiguration getGlobalConfiguration();
}
