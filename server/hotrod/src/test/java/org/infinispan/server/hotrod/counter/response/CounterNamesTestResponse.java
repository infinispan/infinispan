package org.infinispan.server.hotrod.counter.response;

import static java.util.Collections.emptyList;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readString;

import java.util.ArrayList;
import java.util.Collection;

import org.infinispan.server.core.transport.VInt;
import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.test.AbstractTestTopologyAwareResponse;
import org.infinispan.server.hotrod.test.TestResponse;

import io.netty.buffer.ByteBuf;

/**
 * A {@link TestResponse} extension that contains the a collection of counter names.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterNamesTestResponse extends TestResponse {

   private final Collection<String> counterNames;

   public CounterNamesTestResponse(byte version, long messageId, String cacheName, short clientIntel,
         HotRodOperation operation, OperationStatus status,
         int topologyId, AbstractTestTopologyAwareResponse topologyResponse,
         ByteBuf buffer) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId, topologyResponse);
      this.counterNames = readCounterNames(buffer);
   }

   private static Collection<String> readCounterNames(ByteBuf buffer) {
      int size = VInt.read(buffer);
      if (size == 0) {
         return emptyList();
      }
      Collection<String> names = new ArrayList<>(size);
      for (int i = 0; i < size; ++i) {
         names.add(readString(buffer));
      }
      return names;
   }

   public Collection<String> getCounterNames() {
      return counterNames;
   }
}
