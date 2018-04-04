package org.infinispan.client.hotrod.impl.protocol;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedMetadata;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Either;

/**
 * A Hot Rod protocol encoder/decoder.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface Codec {

   /**
    * Writes a request header with the given parameters to the transport and
    * returns an updated header parameters.
    */
   HeaderParams writeHeader(Transport transport, HeaderParams params);

   /**
    * Writes client listener parameters
    */
   void writeClientListenerParams(Transport transport, ClientListener clientListener,
         byte[][] filterFactoryParams, byte[][] converterFactoryParams);

   /**
    * Write lifespan/maxidle parameters.
    */
   void writeExpirationParams(Transport transport, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit);

   /**
    * Reads a response header from the transport and returns the status
    * of the response.
    */
   short readHeader(Transport transport, HeaderParams params);

   ClientEvent readEvent(Transport transport, byte[] expectedListenerId, Marshaller marshaller, List<String> whitelist);

   Either<Short, ClientEvent> readHeaderOrEvent(Transport transport, HeaderParams params, byte[] expectedListenerId, Marshaller marshaller, List<String> whitelist);

   Object returnPossiblePrevValue(Transport transport, short status, int flags, List<String> whitelist);

   /**
    * Logger for Hot Rod client codec
    */
   Log getLog();

   /**
    * Read and unmarshall byte array.
    */
   <T> T readUnmarshallByteArray(Transport transport, short status, List<String> whitelist);

   /**
    * Reads a stream of data
    */
   <T extends InputStream & VersionedMetadata> T readAsStream(Transport transport, VersionedMetadata versionedMetadata, Runnable afterClose);

   /**
    * Writes a stream of data
    */
   OutputStream writeAsStream(Transport transport, Runnable afterClose);

   void writeClientListenerInterests(Transport transport, Set<Class<? extends Annotation>> classes);

   /**
    * Iteration read for projection size
    * @param transport
    * @return
    */
   default int readProjectionSize(Transport transport) {
      return 0;
   }

   /**
    * Iteration read to tell if metadata is present for entry
    * @param transport
    * @return
    */
   default short readMeta(Transport transport) {
      return 0;
   }

   default void writeIteratorStartOperation(Transport transport, Set<Integer> segments, String filterConverterFactory, int batchSize,
         boolean metadata, byte[][] filterParameters) {
      throw new UnsupportedOperationException("This version doesn't support iterating upon entries!");
   }

   /**
    * Creates a key iterator with the given batch size if applicable. This iterator does not support removal.
    * @param remoteCache
    * @param batchSize
    * @param <K>
    * @return
    */
   default <K> CloseableIterator<K> keyIterator(RemoteCache<K, ?> remoteCache, OperationsFactory operationsFactory, int batchSize) {
      throw new UnsupportedOperationException("This version doesn't support iterating upon keys!");
   }

   /**
    * Creates an entry iterator with the given batch size if applicable. This iterator does not support removal.
    * @param remoteCache
    * @param batchSize
    * @param <K>
    * @param <V>
    * @return
    */
   default <K, V> CloseableIterator<Map.Entry<K, V>> entryIterator(RemoteCache<K, V> remoteCache, int batchSize) {
      throw new UnsupportedOperationException("This version doesn't support iterating upon entries!");
   }
}
