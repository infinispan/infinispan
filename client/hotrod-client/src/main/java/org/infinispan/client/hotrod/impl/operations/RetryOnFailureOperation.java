package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.RemoteIllegalLifecycleStateException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for all the operations that need retry logic: if the operation fails due to connection problems, try with 
 * another available connection.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 * @param T the return type of this operation
 */
@Immutable
public abstract class RetryOnFailureOperation<T> extends HotRodOperation {

   private static final Log log = LogFactory.getLog(RetryOnFailureOperation.class, Log.class);

   protected final TransportFactory transportFactory;

   private boolean triedCompleteRestart = false;

   protected RetryOnFailureOperation(Codec codec, TransportFactory transportFactory,
            byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(codec, flags, cacheName, topologyId);
      this.transportFactory = transportFactory;
   }

   @Override
   public T execute() {
      int retryCount = 0;
      Set<SocketAddress> failedServers = null;
      while (shouldRetry(retryCount)) {
         Transport transport = null;
         try {
            // Transport retrieval should be retried
            transport = getTransport(retryCount, failedServers);
            return executeOperation(transport);
         } catch (TransportException te) {
            SocketAddress address = te.getServerAddress();
            failedServers = updateFailedServers(address, failedServers);
            // Invalidate transport since this exception means that this
            // instance is no longer usable and should be destroyed.
            invalidateTransport(transport, address);
            retryCount = logTransportErrorAndThrowExceptionIfNeeded(retryCount, te);
         } catch (RemoteIllegalLifecycleStateException e) {
            SocketAddress address = e.getServerAddress();
            failedServers = updateFailedServers(address, failedServers);
            // Invalidate transport since this exception means that this
            // instance is no longer usable and should be destroyed.
            invalidateTransport(transport, address);
            retryCount = logTransportErrorAndThrowExceptionIfNeeded(retryCount, e);
         } catch (RemoteNodeSuspectException e) {
            // Do not invalidate transport because this exception is caused
            // as a result of a server finding out that another node has
            // been suspected, so there's nothing really wrong with the server
            // from which this node was received.
            logErrorAndThrowExceptionIfNeeded(retryCount, e);
         } finally {
            releaseTransport(transport);
         }

         retryCount++;
      }
      throw new IllegalStateException("We should not reach here!");
   }

   private void invalidateTransport(Transport transport, SocketAddress address) {
      if (transport != null) {
         if (log.isTraceEnabled())
            log.tracef("Invalidating transport %s as a result of transport exception", transport);

         transportFactory.invalidateTransport(address, transport);
      }
   }

   private Set<SocketAddress> updateFailedServers(SocketAddress address, Set<SocketAddress> failedServers) {
      if (failedServers == null) {
         failedServers = new HashSet<SocketAddress>();
      }

      if (log.isTraceEnabled())
         log.tracef("Add %s to failed servers", address);

      failedServers.add(address);
      return failedServers;
   }

   protected boolean shouldRetry(int retryCount) {
      return retryCount <= transportFactory.getMaxRetries();
   }

   protected int logTransportErrorAndThrowExceptionIfNeeded(int i, HotRodClientException e) {
      String message = "Exception encountered. Retry %d out of %d";
      if (i >= transportFactory.getMaxRetries() || transportFactory.getMaxRetries() < 0) {
         if (!triedCompleteRestart) {
            log.debug("Cluster might have completely shut down, try resetting transport layer and topology id", e);
            transportFactory.reset(cacheName);
            triedCompleteRestart = true;
            return -1; // reset retry count
         } else {
            log.exceptionAndNoRetriesLeft(i,transportFactory.getMaxRetries(), e);
            throw e;
         }
      } else {
         log.tracef(e, message, i, transportFactory.getMaxRetries());
         return i;
      }
   }

   protected void logErrorAndThrowExceptionIfNeeded(int i, HotRodClientException e) {
      String message = "Exception encountered. Retry %d out of %d";
      if (i >= transportFactory.getMaxRetries() || transportFactory.getMaxRetries() < 0) {
         log.exceptionAndNoRetriesLeft(i,transportFactory.getMaxRetries(), e);
         throw e;
      } else {
         log.tracef(e, message, i, transportFactory.getMaxRetries());
      }
   }

   protected void releaseTransport(Transport transport) {
      if (transport != null)
         transportFactory.releaseTransport(transport);
   }

   protected abstract Transport getTransport(int retryCount, Set<SocketAddress> failedServers);

   protected abstract T executeOperation(Transport transport);
}
