package org.infinispan.server.resp.response;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.serialization.SerializationHint;

public class LCSResponse {
   public static final BiConsumer<LCSResponse, ResponseWriter> SERIALIZER = (res, writer) ->
         writer.write(res, LcsResponseSerializer.INSTANCE);

   public ArrayList<long[]> idx;
   public byte[] lcs;
   public int[][] C;
   public int len;

   private static final class LcsResponseSerializer implements JavaObjectSerializer<LCSResponse> {
      private static final LcsResponseSerializer INSTANCE = new LcsResponseSerializer();
      private static final byte[] MATCHES = {'m', 'a', 't', 'c', 'h', 'e', 's'};
      private static final byte[] LEN = {'l', 'e', 'n'};

      @Override
      public void accept(LCSResponse res, ResponseWriter writer) {
         // If lcs present, return a bulk_string
         if (res.lcs != null) {
            writer.string(res.lcs);
            return;
         }

         // If idx is null then it's a justLen command, return a long
         if (res.idx == null) {
            writer.integers(res.len);
            return;
         }

         // We need to keep insertion order, since the position of the elements changes their meaning
         Map<byte[], Object> map = new LinkedHashMap<>(2);
         map.put(MATCHES, res.idx);
         map.put(LEN, res.len);

         writer.map(map, new SerializationHint.KeyValueHint(Resp3Type.BULK_STRING, (o, w) -> {
            if (o instanceof List<?>) {
               // The matches
               w.array((List<long[]>) o, (l, w1) -> {
                  if (l.length == 5) {
                     w1.array(
                           List.of(List.of(l[0], l[1]), List.of(l[2], l[3]), l[4]), (ll, w2) -> {
                              if (ll instanceof Collection<?>) {
                                 w2.array((Collection<?>) ll, Resp3Type.INTEGER);
                              } else {
                                 w2.integers((Number) ll);
                              }
                           }
                     );
                  } else {
                     w1.array(
                           List.of(List.of(l[0], l[1]), List.of(l[2], l[3])), (ll, w2) -> {
                              w2.array(ll, Resp3Type.INTEGER);
                           }
                     );
                  }
               });
            } else {
               // The length
               w.integers((Number) o);
            }
         }));
      }
   }
}
