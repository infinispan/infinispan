package org.infinispan.persistence.spi;

import java.util.concurrent.Executor;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.NonBlockingManager;

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
    * Returns a manager that is designed to help with non blocking operations.
    * @return a manager that can be used to help with offloading non blocking work.
    */
   NonBlockingManager getNonBlockingManager();

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

   /**
    * This method returns whether the store can directly purge its contents on startup, which can be more performant than a clear.
    * This will only be true if {@link StoreConfiguration#purgeOnStartup()} is true, but it may be false even when it is
    * if there are other constraints limiting purge until after contents are loaded.
    * @return whether a store can directly purge its contents on startup
    */
   default boolean canStoreDirectlyPurgeOnStartup() {
      return getConfiguration().purgeOnStartup();
   }
}
