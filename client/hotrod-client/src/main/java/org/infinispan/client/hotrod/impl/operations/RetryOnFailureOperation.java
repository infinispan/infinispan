package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for all the operations that need retry logic: if the operation fails due to connection problems, try with 
 * another available connection.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class RetryOnFailureOperation extends HotRodOperation {

   private static Log log = LogFactory.getLog(RetryOnFailureOperation.class);

   protected final TransportFactory transportFactory;

   protected RetryOnFailureOperation(TransportFactory transportFactory, byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(flags, cacheName, topologyId);
      this.transportFactory = transportFactory;
   }

   @Override
   public Object execute() {
      int retryCount = 0;
      Transport transport = getTransport(retryCount);
      do {
         try {
            return executeOperation(transport);
         } catch (TransportException te) {
            logErrorAndThrowExceptionIfNeeded(retryCount, te);
         } finally {
            releaseTransport(transport);
         }
         if (shouldRetry(retryCount)) {
            transport = getTransport(retryCount);
         }
         retryCount++;
      } while (shouldRetry(retryCount));
      throw new IllegalStateException("We should not reach here!");
   }

   protected boolean shouldRetry(int retryCount) {
      return retryCount < transportFactory.getTransportCount();
   }

   protected void logErrorAndThrowExceptionIfNeeded(int i, TransportException te) {
      String message = "Transport exception. Retry " + i + " out of " + transportFactory.getTransportCount();
      if (i == transportFactory.getTransportCount() - 1 || transportFactory.getTransportCount() < 0) {
         log.warn(message, te);
         throw te;
      } else {
         log.trace(message + ":" + te);
      }
   }

   protected void releaseTransport(Transport transport) {
      if (transport != null)
         transportFactory.releaseTransport(transport);
   }

   protected abstract Transport getTransport(int retryCount);

   protected abstract Object executeOperation(Transport transport);
}
