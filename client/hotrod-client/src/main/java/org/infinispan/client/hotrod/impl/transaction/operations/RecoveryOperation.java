package org.infinispan.client.hotrod.impl.transaction.operations;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.transaction.TransactionManager;
import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.transaction.manager.RemoteXid;
import org.infinispan.commons.io.SignedNumeric;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A recovery request from the {@link TransactionManager}.
 * <p>
 * It returns all in-doubt transactions seen by the server.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class RecoveryOperation extends RetryOnFailureOperation<Collection<Xid>> {

   public RecoveryOperation(Codec codec, ChannelFactory channelFactory, AtomicReference<ClientTopology> clientTopology, Configuration cfg) {
      super(FETCH_TX_RECOVERY_REQUEST, FETCH_TX_RECOVERY_RESPONSE, codec, channelFactory, DEFAULT_CACHE_NAME_BYTES,
            clientTopology, 0, cfg, null, null);
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
      codec.writeHeader(buf, header);
      channel.writeAndFlush(buf);
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {
      codec.writeHeader(buf, header);
   }

   private int estimateSize() {
      return codec.estimateHeaderSize(header);
   }

}
