package org.infinispan.hotrod.impl.transaction.operations;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Represents a commit or rollback request from the {@link TransactionManager}.
 *
 * @since 14.0
 */
public class CompleteTransactionOperation extends RetryOnFailureOperation<Integer> {

   private final Xid xid;

   public CompleteTransactionOperation(OperationContext operationContext, Xid xid, boolean commit) {
      super(operationContext, commit ? COMMIT_REQUEST : ROLLBACK_REQUEST, commit ? COMMIT_RESPONSE : ROLLBACK_RESPONSE, CacheOptions.DEFAULT, null);
      this.xid = xid;
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      ByteBuf buf = channel.alloc().buffer(estimateSize());
      operationContext.getCodec().writeHeader(buf, header);
      ByteBufUtil.writeXid(buf, xid);
      channel.writeAndFlush(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (status == NO_ERROR_STATUS) {
         complete(buf.readInt());
      } else {
         complete(XAException.XA_HEURRB);
      }
   }

   private int estimateSize() {
      return operationContext.getCodec().estimateHeaderSize(header) + ByteBufUtil.estimateXidSize(xid);
   }

}
