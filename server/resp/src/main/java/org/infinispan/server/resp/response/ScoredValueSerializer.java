package org.infinispan.server.resp.response;

import java.util.List;

import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.ResponseWriter;

public final class ScoredValueSerializer implements JavaObjectSerializer<ScoredValue<byte[]>> {
   public static final ScoredValueSerializer WITH_SCORE = new ScoredValueSerializer(true);
   public static final ScoredValueSerializer WITHOUT_SCORE = new ScoredValueSerializer(false);
   private final boolean withScore;

   private ScoredValueSerializer(boolean withScore) {
      this.withScore = withScore;
   }

   @Override
   public void accept(ScoredValue<byte[]> sv, ResponseWriter writer) {
      if (withScore) {
         writer.array(List.of(sv.getValue(), sv.score()), (o, w) -> {
            if (o instanceof Number) {
               w.doubles((Number) o);
            } else {
               w.string((byte[]) o);
            }
         });
      } else {
         writer.string(sv.getValue());
      }
   }
}
