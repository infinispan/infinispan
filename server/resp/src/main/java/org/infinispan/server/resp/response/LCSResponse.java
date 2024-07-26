package org.infinispan.server.resp.response;

import java.util.ArrayList;
import java.util.function.BiConsumer;

import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.serialization.ByteBufferUtils;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Response;
import org.infinispan.server.resp.serialization.RespConstants;

public class LCSResponse {
   public static final BiConsumer<LCSResponse, ByteBufPool> SERIALIZER = (res, alloc) ->
         Resp3Response.write(res, alloc, LcsResponseSerializer.INSTANCE);

   public ArrayList<long[]> idx;
   public byte[] lcs;
   public int[][] C;
   public int len;

   private static final class LcsResponseSerializer implements JavaObjectSerializer<LCSResponse> {
      private static final LcsResponseSerializer INSTANCE = new LcsResponseSerializer();
      private static final byte[] MATCHES = {'m', 'a', 't', 'c', 'h', 'e', 's'};
      private static final byte[] LEN = { 'l', 'e', 'n' };

      @Override
      public void accept(LCSResponse res, ByteBufPool alloc) {
         // If lcs present, return a bulk_string
         if (res.lcs != null) {
            Resp3Response.string(res.lcs, alloc);
            return;
         }

         // If idx is null then it's a justLen command, return a long
         if (res.idx == null) {
            Resp3Response.integers(res.len, alloc);
            return;
         }

         // LCS client library for tests assume the keys in this order.
         ByteBufferUtils.writeNumericPrefix(RespConstants.MAP, 2, alloc);

         Resp3Response.string(MATCHES, alloc);
         ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, res.idx.size(), alloc);
         for (long[] match : res.idx) {
            int size = match.length > 4 ? 3 : 2;

            ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, size, alloc);

            ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, 2, alloc);
            Resp3Response.integers(match[0], alloc);
            Resp3Response.integers(match[1], alloc);

            ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, 2, alloc);
            Resp3Response.integers(match[2], alloc);
            Resp3Response.integers(match[3], alloc);

            if (match.length > 4) {
               Resp3Response.integers(match[4], alloc);
            }
         }

         Resp3Response.string(LEN, alloc);
         Resp3Response.integers(res.len, alloc);
      }
   }
}
