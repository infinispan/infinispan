package org.infinispan.server.resp.response;

import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.ResponseWriter;

public final class ScoredValueSerializer implements JavaObjectSerializer<ScoredValue<byte[]>> {
   public static final ScoredValueSerializer INSTANCE = new ScoredValueSerializer();

   @Override
   public void accept(ScoredValue<byte[]> sv, ResponseWriter writer) {
      writer.writeNumericPrefix(RespConstants.ARRAY, 2);

      writer.string(sv.getValue());
      writer.doubles(sv.score());
   }
}
