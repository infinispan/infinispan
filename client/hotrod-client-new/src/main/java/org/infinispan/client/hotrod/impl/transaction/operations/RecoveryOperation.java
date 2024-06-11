package org.infinispan.client.hotrod.impl.transaction.operations;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.Collection;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.impl.operations.AbstractNoCacheHotRodOperation;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.transaction.manager.RemoteXid;
import org.infinispan.commons.io.SignedNumeric;

import io.netty.buffer.ByteBuf;
import jakarta.transaction.TransactionManager;

/**
 * A recovery request from the {@link TransactionManager}.
 * <p>
 * It returns all in-doubt transactions seen by the server.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class RecoveryOperation extends AbstractNoCacheHotRodOperation<Collection<Xid>> {
   @Override
   public Collection<Xid> createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      int size = ByteBufUtil.readVInt(buf);
      if (size == 0) {
         return emptyList();
      }
      Collection<Xid> xids = new ArrayList<>(size);
      for (int i = 0; i < size; ++i) {
         int formatId = SignedNumeric.decode(ByteBufUtil.readVInt(buf));
         byte[] globalId = ByteBufUtil.readArray(buf);
         byte[] branchId = ByteBufUtil.readArray(buf);
         //the Xid class does't matter since it only compares the format-id, global-id and branch-id
         xids.add(RemoteXid.create(formatId, globalId, branchId));
      }
      return xids;
   }

   @Override
   public short requestOpCode() {
      return FETCH_TX_RECOVERY_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return FETCH_TX_RECOVERY_RESPONSE;
   }
}
