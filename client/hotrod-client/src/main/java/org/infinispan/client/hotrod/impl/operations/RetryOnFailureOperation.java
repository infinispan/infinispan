package org.infinispan.client.hotrod.impl.operations;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.RemoteIllegalLifecycleStateException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.DecoderException;
import net.jcip.annotations.Immutable;

/**
 * Base class for all the operations that need retry logic: if the operation fails due to connection problems, try with
 * another available connection.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class RetryOnFailureOperation<T> extends HotRodOperation<T> implements ChannelOperation {

   protected static final Log log = LogFactory.getLog(RetryOnFailureOperation.class, Log.class);
   protected static final boolean trace = log.isTraceEnabled();

   private int retryCount = 0;
   private Set<SocketAddress> failedServers = null;
   private boolean triedCompleteRestart = false;
   private String currentClusterName;

   protected RetryOnFailureOperation(short requestCode, short responseCode, Codec codec, ChannelFactory channelFactory,
                                     byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg,
                                     DataFormat dataFormat) {
      super(requestCode, responseCode, codec, flags, cfg, cacheName, topologyId, channelFactory, dataFormat);
   }

   @Override
   public CompletableFuture<T> execute() {
      assert !isDone();
      try {
         currentClusterName = channelFactory.getCurrentClusterName();
         if (trace) {
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
   public ChannelFuture invoke(Channel channel) {
      try {
         if (trace) {
            log.tracef("About to start executing operation %s on %s", this, channel);
         }
         return executeOperation(channel);
      } catch (Throwable t) {
         completeExceptionally(t);
         return null;
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
         if (trace) {
            log.tracef("Not retrying as done (exceptionally=%s), retryCount=%d", this.isCompletedExceptionally(), retryCount);
         }
      } else {
         reset();
         currentClusterName = channelFactory.getCurrentClusterName();
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
   }

   private Set<SocketAddress> updateFailedServers(SocketAddress address) {
      if (failedServers == null) {
         failedServers = new HashSet<>();
      }

      if (trace)
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
      updateFailedServers(address);
      logAndRetryOrFail(HOTROD.connectionClosed(address, address), true);
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
               if (trace) {
                  log.tracef(cause, "(1) %s Requesting %s close due to exception", this.toString(), channel);
               }
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
            updateFailedServers(address);
         }
         if (channel != null) {
            // We need to remove decoder even if we're about to close the channel
            // because otherwise we would be notified through channelInactive and we would retry (again).
            HeaderDecoder headerDecoder = (HeaderDecoder)channel.pipeline().get(HeaderDecoder.NAME);
            if (headerDecoder != null) {
               channel.pipeline().remove(HeaderDecoder.NAME);
            }
            if (trace) {
               log.tracef(cause, "(2) Requesting %s close due to exception", channel);
            }
            channel.close();
            if (headerDecoder != null) {
               headerDecoder.failoverClientListeners();
            }
         }
         logAndRetryOrFail(cause, true);
         return null;
      } else if (cause instanceof RemoteNodeSuspectException) {
         // Why can't we switch cluster here?
         logAndRetryOrFail(cause, false);
         return null;
      } else if (cause instanceof HotRodClientException && ((HotRodClientException) cause).isServerError()) {
         // fail the operation (don't retry) but don't close the channel
         completeExceptionally(cause);
         return null;
      } else {
         return cause;
      }
   }

   protected void logAndRetryOrFail(Throwable e, boolean canSwitchCluster) {
      if (retryCount < channelFactory.getMaxRetries() && channelFactory.getMaxRetries() >= 0) {
         if (trace) {
            log.tracef(e, "Exception encountered in %s. Retry %d out of %d", this, retryCount, channelFactory.getMaxRetries());
         }
         retryCount++;
         channelFactory.incrementRetryCount();
         retryIfNotDone();
      } else if (canSwitchCluster) {
         channelFactory.trySwitchCluster(currentClusterName, cacheName).whenComplete((status, throwable) -> {
            if (throwable != null) {
               completeExceptionally(throwable);
               return;
            }
            switch (status) {
               case SWITCHED:
                  triedCompleteRestart = true;
                  retryCount = 0;
                  break;
               case NOT_SWITCHED:
                  if (!triedCompleteRestart) {
                     log.debug("Cluster might have completely shut down, try resetting transport layer and topology id", e);
                     channelFactory.reset(cacheName);
                     triedCompleteRestart = true;
                     retryCount = 0;
                  } else {
                     HOTROD.exceptionAndNoRetriesLeft(retryCount, channelFactory.getMaxRetries(), e);
                     completeExceptionally(e);
                  }
                  break;
               case IN_PROGRESS:
                  log.trace("Cluster switch in progress, retry operation without increasing retry count");
                  break;
               default:
                  completeExceptionally(new IllegalStateException("Unknown cluster switch status: " + status));
            }
            retryIfNotDone();
         });
      } else {
         HOTROD.exceptionAndNoRetriesLeft(retryCount, channelFactory.getMaxRetries(), e);
         completeExceptionally(e);
      }
   }

   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      channelFactory.fetchChannelAndInvoke(failedServers, cacheName, this);
   }

   /**
    * Perform the operation-specific request/response I/O on the specified channel.
    * If an error occurs during I/O, this class will detect it and retry the operation with a different channel by invoking the executeOperation method again.
    * @param channel the channel to use for I/O
    */
   protected abstract ChannelFuture executeOperation(Channel channel);

}
