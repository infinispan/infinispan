package org.infinispan.server.resp.response;

import java.util.ArrayList;
import java.util.function.BiConsumer;

import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.ResponseWriter;

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
      private static final byte[] LEN = { 'l', 'e', 'n' };

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

         // LCS client library for tests assume the keys in this order.
         writer.writeNumericPrefix(RespConstants.MAP, 2);

         writer.string(MATCHES);
         writer.writeNumericPrefix(RespConstants.ARRAY, res.idx.size());
         for (long[] match : res.idx) {
            int size = match.length > 4 ? 3 : 2;

            writer.writeNumericPrefix(RespConstants.ARRAY, size);

            writer.writeNumericPrefix(RespConstants.ARRAY, 2);
            writer.integers(match[0]);
            writer.integers(match[1]);

            writer.writeNumericPrefix(RespConstants.ARRAY, 2);
            writer.integers(match[2]);
            writer.integers(match[3]);

            if (match.length > 4) {
               writer.integers(match[4]);
            }
         }

         writer.string(LEN);
         writer.integers(res.len);
      }
   }
}
