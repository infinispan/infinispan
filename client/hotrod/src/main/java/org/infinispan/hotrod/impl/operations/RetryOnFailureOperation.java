package org.infinispan.hotrod.impl.operations;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.exceptions.HotRodClientException;
import org.infinispan.hotrod.exceptions.RemoteIllegalLifecycleStateException;
import org.infinispan.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.hotrod.exceptions.TransportException;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.transport.netty.ChannelOperation;
import org.infinispan.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.channel.Channel;
import io.netty.handler.codec.DecoderException;

/**
 * Base class for all the operations that need retry logic: if the operation fails due to connection problems, try with
 * another available connection.
 *
 * @since 14.0
 */
public abstract class RetryOnFailureOperation<T> extends HotRodOperation<T> implements ChannelOperation {

   protected static final Log log = LogFactory.getLog(RetryOnFailureOperation.class, Log.class);

   private int retryCount = 0;
   private Set<SocketAddress> failedServers = null;

   protected RetryOnFailureOperation(OperationContext operationContext, short requestCode, short responseCode, CacheOptions options, DataFormat dataFormat) {
      super(operationContext, requestCode, responseCode, options, dataFormat);
   }

   @Override
   public CompletionStage<T> execute() {
      assert !isDone();
      try {
         if (log.isTraceEnabled()) {
            log.tracef("Requesting channel for operation %s", this);
         }
         fetchChannelAndInvoke(retryCount, failedServers);
      } catch (Exception e) {
         // if there's a bug before the operation is registered the operation wouldn't be completed
         completeExceptionally(e);
      }
      return this;
   }

   @Override
   public void invoke(Channel channel) {
      try {
         if (log.isTraceEnabled()) {
            log.tracef("About to start executing operation %s on %s", this, channel);
         }
         executeOperation(channel);
      } catch (Throwable t) {
         completeExceptionally(t);
      } finally {
         releaseChannel(channel);
      }
   }

   @Override
   public void cancel(SocketAddress address, Throwable cause) {
      cause = handleException(cause, null, address);
      if (cause != null) {
         completeExceptionally(cause);
      }
   }

   private void retryIfNotDone() {
      if (isDone()) {
         if (log.isTraceEnabled()) {
            log.tracef("Not retrying as done (exceptionally=%s), retryCount=%d", this.isCompletedExceptionally(), retryCount);
         }
      } else {
         reset();
         fetchChannelAndInvoke(retryCount, failedServers);
      }
   }

   // hook for stateful operations
   protected void reset() {
      // The exception may happen when we try to fetch the channel; at this time the operation
      // is not registered yet and timeoutFuture is null
      if (timeoutFuture != null) {
         timeoutFuture.cancel(false);
         timeoutFuture = null;
      }
      // Update the topology age in case the retry is connecting to a new cluster
      header.topologyAge(operationContext.getChannelFactory().getTopologyAge());
   }

   private Set<SocketAddress> addFailedServer(SocketAddress address) {
      if (failedServers == null) {
         failedServers = new HashSet<>();
      }

      if (log.isTraceEnabled())
         log.tracef("Add %s to failed servers", address);

      failedServers.add(address);
      return failedServers;
   }

   @Override
   public void channelInactive(Channel channel) {
      if (isDone()) {
         return;
      }
      SocketAddress address = ChannelRecord.of(channel).getUnresolvedAddress();
      addFailedServer(address);
      logAndRetryOrFail(HOTROD.connectionClosed(address, address));
   }

   @Override
   public void exceptionCaught(Channel channel, Throwable cause) {
      SocketAddress address = channel == null ? null : ChannelRecord.of(channel).getUnresolvedAddress();
      cause = handleException(cause, channel, address);
      if (cause != null) {
         // ctx.close() triggers channelInactive; we want to complete this to signal that no retries are expected
         try {
            completeExceptionally(cause);
         } finally {
            if (channel != null) {
               HOTROD.closingChannelAfterError(channel, cause);
               channel.close();
            }
         }
      }
   }

   protected Throwable handleException(Throwable cause, Channel channel, SocketAddress address) {
      while (cause instanceof DecoderException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      if (cause instanceof RemoteIllegalLifecycleStateException || cause instanceof IOException || cause instanceof TransportException) {
         if (Thread.interrupted()) {
            // Don't invalidate the transport if our thread was interrupted
            completeExceptionally(new InterruptedException());
            return null;
         }
         if (address != null) {
            addFailedServer(address);
         }
         if (channel != null) {
            // We need to remove decoder even if we're about to close the channel
            // because otherwise we would be notified through channelInactive and we would retry (again).
            HeaderDecoder headerDecoder = (HeaderDecoder) channel.pipeline().get(HeaderDecoder.NAME);
            if (headerDecoder != null) {
               channel.pipeline().remove(HeaderDecoder.NAME);
            }
            HOTROD.closingChannelAfterError(channel, cause);
            channel.close();
            if (headerDecoder != null) {
               headerDecoder.failoverClientListeners();
            }
         }
         logAndRetryOrFail(cause);
         return null;
      } else if (cause instanceof RemoteNodeSuspectException) {
         // TODO Clients should never receive a RemoteNodeSuspectException, see ISPN-11636
         logAndRetryOrFail(cause);
         return null;
      } else if (cause instanceof HotRodClientException && ((HotRodClientException) cause).isServerError()) {
         // fail the operation (don't retry) but don't close the channel
         completeExceptionally(cause);
         return null;
      } else {
         return cause;
      }
   }

   protected void logAndRetryOrFail(Throwable e) {
      if (retryCount < operationContext.getChannelFactory().getMaxRetries()) {
         if (log.isTraceEnabled()) {
            log.tracef(e, "Exception encountered in %s. Retry %d out of %d", this, retryCount, operationContext.getChannelFactory().getMaxRetries());
         }
         retryCount++;
         operationContext.getChannelFactory().incrementRetryCount();
         retryIfNotDone();
      } else {
         HOTROD.exceptionAndNoRetriesLeft(retryCount, operationContext.getChannelFactory().getMaxRetries(), e);
         completeExceptionally(e);
      }
   }

   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      operationContext.getChannelFactory().fetchChannelAndInvoke(failedServers, operationContext.getCacheNameBytes(), this);
   }

   /**
    * Perform the operation-specific request/response I/O on the specified channel. If an error occurs during I/O, this
    * class will detect it and retry the operation with a different channel by invoking the executeOperation method
    * again.
    *
    * @param channel the channel to use for I/O
    */
   protected abstract void executeOperation(Channel channel);

}
