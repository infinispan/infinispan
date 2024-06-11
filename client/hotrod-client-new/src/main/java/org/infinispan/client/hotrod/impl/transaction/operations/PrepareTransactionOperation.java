package org.infinispan.client.hotrod.impl.transaction.operations;

import static org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil.writeVInt;
import static org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil.writeXid;

import java.util.List;
import java.util.stream.Collectors;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.AbstractCacheOperation;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import jakarta.transaction.TransactionManager;

/**
 * A prepare request from the {@link TransactionManager}.
 * <p>
 * It contains all the transaction modification to perform the validation.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public class PrepareTransactionOperation extends AbstractCacheOperation<Integer> {

   private final Xid xid;
   private final boolean onePhaseCommit;
   private final List<Modification> modifications;
   private final boolean recoverable;
   private final long timeoutMs;
   private boolean retry;

   public PrepareTransactionOperation(InternalRemoteCache<?, ?> cache, Xid xid, boolean onePhaseCommit,
                                      List<Modification> modifications, boolean recoverable, long timeoutMs) {
      super(cache);
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
   public Integer createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (status == NO_ERROR_STATUS) {
         return buf.readInt();
      } else {
         retry = status == NOT_PUT_REMOVED_REPLACED_STATUS;
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
   public Object getRoutingObject() {
      // We could pass it to the server who owns the first key for a modification maybe?
      return modifications.isEmpty() ? null : modifications.get(0).getKey();
   }

   @Override
   public DataFormat getDataFormat() {
      // Do we need this? It should technically be needed for the modifications, but we never
      // passed one in the prior client impl
      return null;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      writeOperationRequest(buf, codec, xid, onePhaseCommit, recoverable, timeoutMs, modifications);
   }

   public static void writeOperationRequest(ByteBuf buf, Codec codec, Xid xid, boolean onePhaseCommit,
                                            boolean recoverable, long timeoutMs, List<Modification> modifications) {
      writeXid(buf, xid);
      buf.writeBoolean(onePhaseCommit);
      buf.writeBoolean(recoverable);
      buf.writeLong(timeoutMs);
      writeVInt(buf, modifications.size());
      for (Modification m : modifications) {
         m.writeTo(buf, codec);
      }
   }

   @Override
   protected void addParams(StringBuilder sb) {
      sb.append(", modifications=[");
      sb.append(modifications.stream().map(Object::toString)
            .collect(Collectors.joining(",")).toString());
      sb.append("]");
   }
}
