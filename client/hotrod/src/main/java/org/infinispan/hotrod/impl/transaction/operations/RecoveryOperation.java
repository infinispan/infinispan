package org.infinispan.hotrod.impl.transaction.operations;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.Collection;

import javax.transaction.TransactionManager;
import javax.transaction.xa.Xid;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.commons.io.SignedNumeric;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.hotrod.transaction.manager.RemoteXid;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A recovery request from the {@link TransactionManager}.
 * <p>
 * It returns all in-doubt transactions seen by the server.
 *
 * @since 14.0
 */
public class RecoveryOperation extends RetryOnFailureOperation<Collection<Xid>> {

   public RecoveryOperation(OperationContext operationContext) {
      super(operationContext, FETCH_TX_RECOVERY_REQUEST, FETCH_TX_RECOVERY_RESPONSE, CacheOptions.DEFAULT, null);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (status != NO_ERROR_STATUS) {
         complete(emptyList());
         return;
      }
      int size = ByteBufUtil.readVInt(buf);
      if (size == 0) {
         complete(emptyList());
         return;
      }
      Collection<Xid> xids = new ArrayList<>(size);
      for (int i = 0; i < size; ++i) {
         int formatId = SignedNumeric.decode(ByteBufUtil.readVInt(buf));
         byte[] globalId = ByteBufUtil.readArray(buf);
         byte[] branchId = ByteBufUtil.readArray(buf);
         //the Xid class does't matter since it only compares the format-id, global-id and branch-id
         xids.add(RemoteXid.create(formatId, globalId, branchId));
      }
      complete(xids);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      ByteBuf buf = channel.alloc().buffer(estimateSize());
      operationContext.getCodec().writeHeader(buf, header);
      channel.writeAndFlush(buf);
   }

   private int estimateSize() {
      return operationContext.getCodec().estimateHeaderSize(header);
   }

}
