package org.infinispan.client.hotrod.impl.transaction.operations;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.impl.operations.AbstractNoCacheHotRodOperation;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import jakarta.transaction.TransactionManager;

/**
 * Represents a commit or rollback request from the {@link TransactionManager}.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public class CompleteTransactionOperation extends AbstractNoCacheHotRodOperation<Integer> {

   private final Xid xid;
   private final boolean commit;

   public CompleteTransactionOperation(Xid xid, boolean commit) {
      this.xid = xid;
      this.commit = commit;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeXid(buf, xid);
   }

   @Override
   public Integer createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (status == HotRodConstants.NO_ERROR_STATUS) {
         return buf.readInt();
      }
      return XAException.XA_HEURRB;
   }

   @Override
   public short requestOpCode() {
      return commit ? HotRodConstants.COMMIT_REQUEST : HotRodConstants.ROLLBACK_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return commit ? HotRodConstants.COMMIT_RESPONSE : HotRodConstants.ROLLBACK_RESPONSE;
   }
}
