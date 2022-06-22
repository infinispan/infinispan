package org.infinispan.client.hotrod.impl.operations;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.RemoteIllegalLifecycleStateException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.telemetry.impl.TelemetryService;

import io.netty.channel.Channel;
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

   private int retryCount = 0;
   private Set<SocketAddress> failedServers = null;

   protected RetryOnFailureOperation(short requestCode, short responseCode, Codec codec, ChannelFactory channelFactory,
                                     byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg,
                                     DataFormat dataFormat, TelemetryService telemetryService) {
      super(requestCode, responseCode, codec, flags, cfg, cacheName, clientTopology, channelFactory, dataFormat);
      if (telemetryService != null) {
         telemetryService.injectSpanContext(header);
      }
   }

   @Override
   public CompletableFuture<T> execute() {
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
      header.topologyAge(channelFactory.getTopologyAge());
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
         if (channel != null && closeChannelForCause(cause)) {
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
      } else if (isServerError(cause)) {
         // fail the operation (don't retry) but don't close the channel
         completeExceptionally(cause);
         return null;
      } else {
         return cause;
      }
   }

   protected void logAndRetryOrFail(Throwable e) {
      if (retryCount < channelFactory.getMaxRetries()) {
         if (log.isTraceEnabled()) {
            log.tracef(e, "Exception encountered in %s. Retry %d out of %d", this, retryCount, channelFactory.getMaxRetries());
         }
         retryCount++;
         channelFactory.incrementRetryCount();
         retryIfNotDone();
      } else {
         HOTROD.exceptionAndNoRetriesLeft(retryCount, channelFactory.getMaxRetries(), e);
         completeExceptionally(e);
      }
   }

   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      channelFactory.fetchChannelAndInvoke(failedServers, cacheName(), this);
   }

   /**
    * Perform the operation-specific request/response I/O on the specified channel.
    * If an error occurs during I/O, this class will detect it and retry the operation with a different channel by invoking the executeOperation method again.
    * @param channel the channel to use for I/O
    */
   protected abstract void executeOperation(Channel channel);

}
