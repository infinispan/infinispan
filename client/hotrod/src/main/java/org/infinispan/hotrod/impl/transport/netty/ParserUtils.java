package org.infinispan.hotrod.impl.transport.netty;

import static org.infinispan.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_CREATED_EVENT_RESPONSE;
import static org.infinispan.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_EXPIRED_EVENT_RESPONSE;
import static org.infinispan.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_MODIFIED_EVENT_RESPONSE;
import static org.infinispan.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_REMOVED_EVENT_RESPONSE;
import static org.infinispan.hotrod.impl.protocol.HotRodConstants.COUNTER_EVENT_RESPONSE;

public class ParserUtils {

   public static boolean isEntryEventOp(short opCode) {
      return opCode == CACHE_ENTRY_CREATED_EVENT_RESPONSE
            || opCode == CACHE_ENTRY_MODIFIED_EVENT_RESPONSE
            || opCode == CACHE_ENTRY_REMOVED_EVENT_RESPONSE
            || opCode == CACHE_ENTRY_EXPIRED_EVENT_RESPONSE;
   }

   public static boolean isCounterEventOp(short opCode) {
      return opCode == COUNTER_EVENT_RESPONSE;
   }
}
