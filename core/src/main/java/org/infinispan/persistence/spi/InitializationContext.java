package org.infinispan.persistence.spi;

import java.util.concurrent.ExecutorService;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.util.TimeService;

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
    * @deprecated This method will be removed in the future and {@link #getUserMarshaller()}} should be used instead.
    * @return the results of {@link #getUserMarshaller()} if the user marshaller implements the {@link StreamingMarshaller}
    * interface, otherwise {@link UnsupportedOperationException};
    */
   @Deprecated
   StreamingMarshaller getMarshaller();

   Marshaller getUserMarshaller();

   TimeService getTimeService();

   /**
    * To be used for building {@link org.infinispan.commons.io.ByteBuffer} objects.
    */
   ByteBufferFactory getByteBufferFactory();

   /**
    * To be used for building {@link org.infinispan.marshall.core.MarshalledEntry} objects.
    */
   MarshalledEntryFactory getMarshalledEntryFactory();

   /**
    * Returns the preferred executor to be used by stores if needed. Stores normally shouldn't need this unless they
    * *must* perform some blocking code asynchronously.
    * @return the executor to be used with stores
    */
   ExecutorService getExecutor();
}
