package org.infinispan.client.hotrod.impl.transaction.operations;

import java.util.concurrent.atomic.AtomicReference;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * It forgets the transaction identified by {@link Xid} in the server.
 * <p>
 * It affects all caches involved in the transaction. It is requested from {@link XAResource#forget(Xid)}.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class ForgetTransactionOperation extends RetryOnFailureOperation<Void> {
   private final Xid xid;

   public ForgetTransactionOperation(Codec codec, ChannelFactory channelFactory, AtomicReference<ClientTopology> clientTopology,
         Configuration cfg, Xid xid) {
      super(FORGET_TX_REQUEST, FORGET_TX_RESPONSE, codec, channelFactory, DEFAULT_CACHE_NAME_BYTES, clientTopology, 0, cfg,
            null, null);
      this.xid = xid;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(null);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      ByteBuf buf = channel.alloc().buffer(estimateSize());
      codec.writeHeader(buf, header);
      ByteBufUtil.writeXid(buf, xid);
      channel.writeAndFlush(buf);
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {
      codec.writeHeader(buf, header);
      ByteBufUtil.writeXid(buf, xid);
   }

   private int estimateSize() {
      return codec.estimateHeaderSize(header) + ByteBufUtil.estimateXidSize(xid);
   }

}
