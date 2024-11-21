package org.infinispan.client.hotrod.impl.operations;

import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transaction.operations.PrepareTransactionOperation;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class NoCachePrepareTransactionOperation extends AbstractNoCacheHotRodOperation<Integer> {
   private final String cacheName;
   private final Xid xid;
   private final boolean onePhaseCommit;
   private final List<Modification> modifications;
   private final boolean recoverable;
   private final long timeoutMs;

   public NoCachePrepareTransactionOperation(String cacheName, Xid xid, boolean onePhaseCommit,
                                             List<Modification> modifications, boolean recoverable, long timeoutMs) {
      this.cacheName = cacheName;
      this.xid = xid;
      this.onePhaseCommit = onePhaseCommit;
      this.modifications = modifications;
      this.recoverable = recoverable;
      this.timeoutMs = timeoutMs;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      PrepareTransactionOperation.writeOperationRequest(buf, codec, xid, onePhaseCommit, recoverable, timeoutMs, modifications);
   }

   @Override
   public Integer createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (status == NO_ERROR_STATUS) {
         return buf.readInt();
      } else {
         return 0;
      }
   }

   @Override
   public short requestOpCode() {
      return PREPARE_TX_2_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return PREPARE_TX_2_RESPONSE;
   }

   @Override
   public String getCacheName() {
      return cacheName;
   }

   @Override
   public byte[] getCacheNameBytes() {
      return cacheName.getBytes(StandardCharsets.UTF_8);
   }
}
