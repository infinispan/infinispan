package org.infinispan.server.hotrod.counter.response;

import static java.util.Collections.emptyList;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readRangedBytes;

import java.util.ArrayList;
import java.util.Collection;

import javax.transaction.xa.Xid;

import org.infinispan.commons.io.SignedNumeric;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.server.core.transport.VInt;
import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.test.AbstractTestTopologyAwareResponse;
import org.infinispan.server.hotrod.test.TestResponse;

import io.netty.buffer.ByteBuf;

/**
 * A {@link TestResponse} extension that contains the list of {@link Xid} to recover.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class RecoveryTestResponse extends TestResponse {

   private final Collection<Xid> xids;

   public RecoveryTestResponse(byte version, long messageId, String cacheName, short clientIntel,
         HotRodOperation operation, OperationStatus status, int topologyId,
         AbstractTestTopologyAwareResponse topologyResponse, ByteBuf buffer) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId, topologyResponse);
      this.xids = readXids(buffer);
   }

   private static Collection<Xid> readXids(ByteBuf buffer) {
      int size = VInt.read(buffer);
      if (size == 0) {
         return emptyList();
      }
      Collection<Xid> xids = new ArrayList<>(size);
      for (int i = 0; i < size; ++i) {
         int formatId = SignedNumeric.decode(VInt.read(buffer));
         byte[] globalId = readRangedBytes(buffer);
         xids.add(XidImpl.create(formatId, globalId));
      }
      return xids;
   }

   public Collection<Xid> getXids() {
      return xids;
   }
}
