package org.infinispan.client.hotrod.event.impl;

import java.lang.annotation.Annotation;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.impl.operations.AddPrimaryNearCacheClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.ClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;

public final class PrimaryNearCacheEventDispatcher extends ClientEventDispatcher {

   private final AddPrimaryNearCacheClientListenerOperation primaryOp;
   private final InvalidatedNearRemoteCache<?, ?> inrc;

   PrimaryNearCacheEventDispatcher(AddPrimaryNearCacheClientListenerOperation op, SocketAddress address,
                                   Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables,
                                   Runnable cleanup, InvalidatedNearRemoteCache<?, ?> inrc) {
      super(op, address, invocables, inrc.getName(), cleanup, inrc);
      this.primaryOp = op;
      this.inrc = inrc;
   }

   public static PrimaryNearCacheEventDispatcher create(AddPrimaryNearCacheClientListenerOperation op,
                                                        SocketAddress address, Runnable cleanup,
                                                        InvalidatedNearRemoteCache<?, ?> inrc) {
      Map<Class<? extends Annotation>, List<ClientListenerInvocation>> invocables = findMethods(op.listener);
      return new PrimaryNearCacheEventDispatcher(op, address, invocables, cleanup, inrc);
   }

   public static PrimaryNearCacheEventDispatcher create(AddPrimaryNearCacheClientListenerOperation op, Runnable cleanup) {
      return create(op, null, cleanup, (InvalidatedNearRemoteCache<?, ?>) op.getRemoteCache());
   }

   public static PrimaryNearCacheEventDispatcher create(AddPrimaryNearCacheClientListenerOperation op,
                                                        InvalidatedNearRemoteCache<?, ?> inrc) {
      return create(op, null, null, inrc);
   }

   public static PrimaryNearCacheEventDispatcher create(ClientListenerOperation op,
                                                        InvalidatedNearRemoteCache<?, ?> inrc) {
      return create((AddPrimaryNearCacheClientListenerOperation) op, inrc);
   }

   @Override
   public CompletableFuture<Void> executeFailover(SocketAddress failedAddress) {
      if (!isRunning() || !inrc.isPrimaryMode()) {
         return CompletableFuture.completedFuture(null);
      }
      Map<SocketAddress, Set<Integer>> primarySegments = inrc.getDispatcher().getPrimarySegmentsByAddress(inrc.getName());
      boolean isPrimary = primarySegments != null && primarySegments.containsKey(failedAddress);
      if (!isPrimary) {
         // Node is no longer a primary owner; complete cleanly without retrying
         return CompletableFuture.completedFuture(null);
      }

      inrc.getDispatcher().getClientListenerNotifier().addDispatcher(this);
      OperationDispatcher dispatcher = inrc.getDispatcher();
      return dispatcher.executeAddListener(primaryOp.copy(), failedAddress)
            .thenCompose(channel -> {
               if (inrc.getNearCacheConfiguration().bloomFilter()) {
                  byte[] bloomBits = inrc.getNearCacheService().calculateBloomBits();
                  if (bloomBits != null) {
                     HotRodOperation<Void> updateOp = inrc.getOperationsFactory().newUpdateBloomFilterOperation(bloomBits);
                     return dispatcher.executeOnSingleAddress(updateOp, ChannelRecord.of(channel))
                           .thenRun(() -> inrc.setServerListenerActive(failedAddress, channel, true));
                  }
               }
               inrc.setServerListenerActive(failedAddress, channel, true);
               return CompletableFuture.completedFuture(null);
            })
            .toCompletableFuture();
   }

   @Override
   protected void invokeFailoverEvent(SocketAddress failedAddress) {
      inrc.handlePrimaryListenerDisconnected(failedAddress);
   }
}
