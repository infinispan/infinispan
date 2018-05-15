package org.infinispan.client.hotrod.transaction.manager;

import static org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil.estimateVIntSize;
import static org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil.writeArray;
import static org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil.writeSignedVInt;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.Xid;

import org.infinispan.commons.tx.XidImpl;

import io.netty.buffer.ByteBuf;

/**
 * Implementation of {@link Xid} used by {@link RemoteTransactionManager}.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public final class RemoteXid extends XidImpl {

   //HRTX in hex
   private static final int FORMAT_ID = 0x48525458;
   private static final AtomicLong GLOBAL_ID_GENERATOR = new AtomicLong(1);
   private static final AtomicLong BRANCH_QUALIFIER_GENERATOR = new AtomicLong(1);

   private RemoteXid(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
      super(formatId, globalTransactionId, branchQualifier);
   }


   public static RemoteXid create(UUID tmId) {
      long creationTime = System.currentTimeMillis();
      byte[] gid = create(tmId, creationTime, GLOBAL_ID_GENERATOR);
      byte[] bid = create(tmId, creationTime, BRANCH_QUALIFIER_GENERATOR);
      return new RemoteXid(FORMAT_ID, gid, bid);
   }

   private static void longToBytes(long val, byte[] array, int offset) {
      for (int i = 7; i > 0; i--) {
         array[offset + i] = (byte) val;
         val >>>= 8;
      }
      array[offset] = (byte) val;
   }

   private static byte[] create(UUID transactionManagerId, long creatingTime, AtomicLong generator) {
      byte[] field = new byte[32]; //size of 4 longs
      longToBytes(transactionManagerId.getLeastSignificantBits(), field, 0);
      longToBytes(transactionManagerId.getMostSignificantBits(), field, 8);
      longToBytes(creatingTime, field, 16);
      longToBytes(generator.getAndIncrement(), field, 24);
      return field;
   }

   public void writeTo(ByteBuf byteBuf) {
      writeSignedVInt(byteBuf, FORMAT_ID);
      byte[] rawData = rawData();
      writeArray(byteBuf, rawData, globalIdOffset(), globalIdLength());
      writeArray(byteBuf, rawData, branchQualifierOffset(), branchQualifierLength());
   }

   public int estimateSize() {
      return estimateVIntSize(FORMAT_ID) + globalIdLength() + branchQualifierLength();
   }
}
