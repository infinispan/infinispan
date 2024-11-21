package org.infinispan.client.hotrod.impl.transaction.operations;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.impl.operations.AbstractNoCacheHotRodOperation;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
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
public class ForgetTransactionOperation extends AbstractNoCacheHotRodOperation<Void> {
   private final Xid xid;

   public ForgetTransactionOperation(Xid xid) {
      this.xid = xid;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeXid(buf, xid);
   }

   @Override
   public Void createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return null;
   }

   @Override
   public short requestOpCode() {
      return HotRodConstants.FORGET_TX_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.FORGET_TX_RESPONSE;
   }

   @Override
   public boolean supportRetry() {
      // We want to retry to other servers as the tx should have been replicated
      return true;
   }
}
