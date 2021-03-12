package org.infinispan.persistence.spi;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.util.concurrent.BlockingManager;

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

   TimeService getTimeService();

   /**
    * To be used for building {@link org.infinispan.commons.io.ByteBuffer} objects.
    */
   ByteBufferFactory getByteBufferFactory();

   /**
    * Returns the preferred executor to be used by stores if needed. Stores normally shouldn't need this unless they
    * *must* perform some blocking code asynchronously.
    * @return the executor to be used with stores
    * @deprecated since 11.0 - Please use {@link #getBlockingManager()} ()} or {@link #getNonBlockingExecutor()} instead
    */
   @Deprecated
   ExecutorService getExecutor();

   /**
    * Returns an executor for non-blocking tasks. Users must guarantee that the tasks they submit to this executor
    * do not block the thread in which the executor runs. Doing so can cause Infinispan to handle operations
    * more slowly, reducing performance, because threads are limited to the number of cores and are used extensively.
    * @return an executor that can submit non-blocking tasks.
    */
   Executor getNonBlockingExecutor();

   /**
    * Returns a manager that is designed to execute tasks that might block. This manager ensures that only the blocking
    * code is run on a blocking thread and any stage continues on a non-blocking thread.
    * @return a manager that should be used to execute blocking operations.
    */
   BlockingManager getBlockingManager();

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
