package org.infinispan.client.hotrod.impl.operations;

import static org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil.estimateVIntSize;
import static org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil.estimateXidSize;
import static org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil.writeVInt;
import static org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil.writeXid;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.TransactionManager;
import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A prepare request from the {@link TransactionManager}.
 * <p>
 * It contains all the transaction modification to perform the validation.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public class PrepareTransactionOperation extends RetryOnFailureOperation<Integer> {

   private final Xid xid;
   private final boolean onePhaseCommit;
   private final Collection<Modification> modifications;
   private boolean retry;

   PrepareTransactionOperation(Codec codec, ChannelFactory channelFactory, byte[] cacheName,
         AtomicInteger topologyId, Configuration cfg, Xid xid, boolean onePhaseCommit,
         Collection<Modification> modifications) {
      super(PREPARE_REQUEST, PREPARE_RESPONSE, codec, channelFactory, cacheName, topologyId, 0, cfg, null);
      this.xid = xid;
      this.onePhaseCommit = onePhaseCommit;
      this.modifications = modifications;
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
      ByteBuf buf = channel.alloc().buffer(estimateSize());
      codec.writeHeader(buf, header);
      writeXid(buf, xid);
      buf.writeByte(onePhaseCommit ? 1 : 0);
      writeVInt(buf, modifications.size());
      for (Modification m : modifications) {
         m.writeTo(buf, codec);
      }
      channel.writeAndFlush(buf);
   }

   private int estimateSize() {
      int size = codec.estimateHeaderSize(header) + estimateXidSize(xid) + 1 +estimateVIntSize(modifications.size());
      for (Modification modification : modifications) {
         size += modification.estimateSize(codec);
      }
      return size;
   }
}
