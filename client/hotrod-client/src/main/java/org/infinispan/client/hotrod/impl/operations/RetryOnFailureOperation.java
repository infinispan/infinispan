package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.commons.util.concurrent.RetriableNotifyingFuture;

/**
 * Base class for all the operations that need retry logic: if the operation fails due to connection problems, try with 
 * another available connection.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 * @param T the return type of this operation
 */
@Immutable
public abstract class RetryOnFailureOperation<T> extends HotRodOperation<T> {

   private static final Log log = LogFactory.getLog(RetryOnFailureOperation.class, Log.class);

   protected final TransportFactory transportFactory;

   protected RetryOnFailureOperation(Codec codec, TransportFactory transportFactory,
            byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(codec, flags, cacheName, topologyId);
      this.transportFactory = transportFactory;
   }

   @Override
   public NotifyingFuture<T> executeAsync() {
      RetryContext<T> context = new RetryContext<T>();
      startExecution(context);
      return new RetriableNotifyingFuture<T>(context.requestFuture, new RetryChecker(context));
   }

   private void startExecution(final RetryContext<T> context) {
      while (shouldRetry(context.retryCount)) {
         context.requestFuture = null;
         // Note: after we handover the execution to another thread, we must not
         // increase retry count without proper synchronization. That's why we
         // increment it here first and then use context.retryCount - 1
         context.retryCount++;
         try {
            if (invokeChecked(new Runnable() {
               @Override
               public void run() {
                  context.transport = getTransport(context.retryCount - 1, context.failedServers);
                  final HeaderParams params = writeRequest(context.transport);
                  context.requestFuture = context.transport.flush(new Callable<T>() {
                     @Override
                     public T call() throws Exception {
                        return readResponse(context.transport, params);
                     }
                  });
               }
            }, context)) {
               return;
            } else {
               // note: if we have invalidated the transport in invokeChecked due to TransportException,
               // we invalidate (destroy) it second time here. The transport should be prepared to that.
               releaseTransport(context.transport);
            }
         } catch (RuntimeException e) {
            releaseTransport(context.transport);
            throw e; // invokeChecked catches everything after what we should retry
         }
      }
   }

   private boolean invokeChecked(Runnable runnable, RetryContext context) {
      try {
         runnable.run();
         return true;
      } catch (TransportException te) {
         if (context.failedServers == null) {
            context.failedServers = new HashSet<SocketAddress>();
         }
         context.failedServers.add(te.getServerAddress());
         // Invalidate transport since this exception means that this
         // instance is no longer usable and should be destroyed.
         if (context.transport != null) {
            transportFactory.invalidateTransport(
                  te.getServerAddress(), context.transport);
         }
         logErrorAndThrowExceptionIfNeeded(context.retryCount - 1, te);
         return false;
      } catch (RemoteNodeSuspectException e) {
         // Do not invalidate transport because this exception is caused
         // as a result of a server finding out that another node has
         // been suspected, so there's nothing really wrong with the server
         // from which this node was received.
         logErrorAndThrowExceptionIfNeeded(context.retryCount - 1, e);
         return false;
      }
   }

   private static class RetryContext<T> {
      int retryCount = 0;
      Set<SocketAddress> failedServers = null;
      Transport transport;
      NotifyingFuture<T> requestFuture;
   }

   private class RetryChecker implements RetriableNotifyingFuture.Checker<T> {
      final RetryContext<T> context;

      private RetryChecker(RetryContext<T> context) {
         this.context = context;
      }

      @Override
      public boolean check(final Future<T> future) {
         try {
            return invokeChecked(new Runnable() {
               @Override
               public void run() {
                  try {
                     future.get();
                  } catch (InterruptedException e) {
                     Thread.currentThread().interrupt();
                  } catch (ExecutionException e) {
                     if (e.getCause() instanceof RuntimeException) {
                        throw (RuntimeException) e.getCause();
                     } else {
                        throw new RuntimeException(e.getCause());
                     }
                  }
               }
            }, context) || Thread.interrupted() || !shouldRetry(context.retryCount);
         } finally {
            releaseTransport(context.transport);
         }
      }

      @Override
      public NotifyingFuture<T> retry() {
         startExecution(context);
         return context.requestFuture;
      }
   }

   protected boolean shouldRetry(int retryCount) {
      return retryCount <= transportFactory.getMaxRetries();
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

   protected abstract HeaderParams writeRequest(Transport transport);
   protected abstract T readResponse(Transport transport, HeaderParams params);
}
