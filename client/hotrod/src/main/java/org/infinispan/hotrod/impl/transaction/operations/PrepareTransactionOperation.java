package org.infinispan.hotrod.impl.transaction.operations;

import static org.infinispan.hotrod.impl.transport.netty.ByteBufUtil.estimateVIntSize;
import static org.infinispan.hotrod.impl.transport.netty.ByteBufUtil.estimateXidSize;
import static org.infinispan.hotrod.impl.transport.netty.ByteBufUtil.writeVInt;
import static org.infinispan.hotrod.impl.transport.netty.ByteBufUtil.writeXid;

import java.net.SocketAddress;
import java.util.List;
import java.util.Set;

import javax.transaction.TransactionManager;
import javax.transaction.xa.Xid;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transaction.entry.Modification;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A prepare request from the {@link TransactionManager}.
 * <p>
 * It contains all the transaction modification to perform the validation.
 *
 * @since 14.0
 */
public class PrepareTransactionOperation extends RetryOnFailureOperation<Integer> {

   private final Xid xid;
   private final boolean onePhaseCommit;
   private final List<Modification> modifications;
   private final boolean recoverable;
   private final long timeoutMs;
   private boolean retry;

   public PrepareTransactionOperation(OperationContext operationContext, Xid xid, boolean onePhaseCommit,
                                      List<Modification> modifications, boolean recoverable, long timeoutMs) {
      super(operationContext, PREPARE_TX_2_REQUEST, PREPARE_TX_2_RESPONSE, CacheOptions.DEFAULT, null);
      this.xid = xid;
      this.onePhaseCommit = onePhaseCommit;
      this.modifications = modifications;
      this.recoverable = recoverable;
      this.timeoutMs = timeoutMs;
   }

   public boolean shouldRetry() {
      return retry;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (status == NO_ERROR_STATUS) {
         complete(buf.readInt());
      } else {
         retry = status == NOT_PUT_REMOVED_REPLACED_STATUS;
         complete(0);
      }
   }

   @Override
   protected void executeOperation(Channel channel) {
      retry = false;
      scheduleRead(channel);
      Codec codec = operationContext.getCodec();
      ByteBuf buf = channel.alloc().buffer(estimateSize(codec));
      codec.writeHeader(buf, header);
      writeXid(buf, xid);
      buf.writeBoolean(onePhaseCommit);
      buf.writeBoolean(recoverable);
      buf.writeLong(timeoutMs);
      writeVInt(buf, modifications.size());
      for (Modification m : modifications) {
         m.writeTo(buf, codec);
      }
      channel.writeAndFlush(buf);
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      if (modifications.isEmpty()) {
         super.fetchChannelAndInvoke(retryCount, failedServers);
      } else {
         operationContext.getChannelFactory().fetchChannelAndInvoke(modifications.get(0).getKey(), failedServers, operationContext.getCacheNameBytes(), this);
      }
   }

   private int estimateSize(Codec codec) {
      int size = codec.estimateHeaderSize(header) + estimateXidSize(xid) + 1 + estimateVIntSize(modifications.size());
      for (Modification modification : modifications) {
         size += modification.estimateSize(codec);
      }
      return size;
   }
}
