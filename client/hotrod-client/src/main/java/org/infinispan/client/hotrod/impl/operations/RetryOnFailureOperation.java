package org.infinispan.client.hotrod.impl.operations;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.RemoteIllegalLifecycleStateException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
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

   protected RetryOnFailureOperation(Codec codec, ChannelFactory channelFactory,
                                     byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, flags, cfg, cacheName, topologyId, channelFactory);
   }

   @Override
   public CompletableFuture<T> execute() {
      assert !isDone();
      currentClusterName = channelFactory.getCurrentClusterName();
      if (trace) {
         log.tracef("Requesting channel for operation %s", this);
      }
      fetchChannelAndInvoke(retryCount, failedServers);
      return this;
   }

   @Override
   public void invoke(Channel channel) {
      assert channel.isActive();
      try {
         if (trace) {
            log.tracef("About to start executing operation %s on %s", this, channel);
         }
         executeOperation(channel);
      } catch (Throwable t) {
         completeExceptionally(t);
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
      if (!isDone()) {
         reset();
         currentClusterName = channelFactory.getCurrentClusterName();
         fetchChannelAndInvoke(retryCount, failedServers);
      }
   }

   protected void reset() {
      // hook for stateful operations
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
   public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      if (isDone()) {
         return;
      }
      SocketAddress address = ChannelRecord.of(ctx.channel()).getUnresolvedAddress();
      updateFailedServers(address);
      logAndRetryOrFail(log.connectionClosed(address, address), true);
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      SocketAddress address = ctx == null ? null : ChannelRecord.of(ctx.channel()).getUnresolvedAddress();
      cause = handleException(cause, ctx, address);
      if (cause != null) {
         // ctx.close() triggers channelInactive; we want to complete this to signal that no retries are expected
         try {
            completeExceptionally(cause);
         } finally {
            if (ctx != null) {
               ctx.pipeline().remove(this);
               ctx.close();
            }
         }
      }
   }

   protected Throwable handleException(Throwable cause, ChannelHandlerContext ctx, SocketAddress address) {
      while (cause instanceof DecoderException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      if (cause instanceof RemoteIllegalLifecycleStateException || cause instanceof IOException || cause instanceof TransportException) {
         if (address != null) {
            updateFailedServers(address);
         }
         if (ctx != null) {
            // We need to remove self even if we're about to close the channel
            // because otherwise we would be notified through channelInactive and we would retry (again).
            ctx.pipeline().remove(this);
            ctx.close();
         }
         logAndRetryOrFail(cause, true);
         return null;
      } else if (cause instanceof RemoteNodeSuspectException) {
         if (ctx != null) {
            ctx.pipeline().remove(this);
            releaseChannel(ctx.channel());
         }
         logAndRetryOrFail(cause, false);
         return null;
      } else {
         return cause;
      }
   }

   protected void logAndRetryOrFail(Throwable e, boolean canSwitchCluster) {
      if (retryCount < channelFactory.getMaxRetries() && channelFactory.getMaxRetries() >= 0) {
         String message = "Exception encountered. Retry %d out of %d";
         log.tracef(e, message, retryCount, channelFactory.getMaxRetries());
         retryCount++;
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
                     log.exceptionAndNoRetriesLeft(retryCount, channelFactory.getMaxRetries(), e);
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
         log.exceptionAndNoRetriesLeft(retryCount, channelFactory.getMaxRetries(), e);
         completeExceptionally(e);
      }
   }

   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      channelFactory.fetchChannelAndInvoke(failedServers, cacheName, this);
   }

   protected abstract void executeOperation(Channel channel);

}
