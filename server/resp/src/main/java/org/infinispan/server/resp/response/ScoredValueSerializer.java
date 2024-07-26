package org.infinispan.server.resp.response;

import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.serialization.ByteBufferUtils;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Response;
import org.infinispan.server.resp.serialization.RespConstants;

public final class ScoredValueSerializer implements JavaObjectSerializer<ScoredValue<byte[]>> {
   public static final ScoredValueSerializer INSTANCE = new ScoredValueSerializer();

   @Override
   public void accept(ScoredValue<byte[]> sv, ByteBufPool alloc) {
      ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, 2, alloc);

      Resp3Response.string(sv.getValue(), alloc);
      Resp3Response.doubles(sv.score(), alloc);
   }
}
