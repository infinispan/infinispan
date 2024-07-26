package org.infinispan.server.resp.commands.sortedset;

import java.util.Arrays;
import java.util.Collection;

import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.Util;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.serialization.ByteBufferUtils;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Response;
import org.infinispan.server.resp.serialization.RespConstants;

public final class ZSetCommonUtils {
   public static final byte[] WITHSCORES = "WITHSCORES".getBytes();
   public static byte EXCLUDE = ((byte)'(');

   private ZSetCommonUtils() {
   }

   public static boolean isWithScoresArg(byte[] arg) {
      return Util.isAsciiBytesEquals(WITHSCORES, arg);
   }

   private static boolean isInf(byte[] arg, char sign) {
      if (arg.length != 4)
         return false;

      return arg[0] == (byte) sign && arg[1] == (byte) 'i' && arg[2] == (byte) 'n' && arg[3] == (byte) 'f';
   }

   public static class Score {
      public boolean unboundedMin;
      public boolean unboundedMax;
      public boolean include = true;
      public Double value;
   }

   public static class Lex {
      public boolean include;
      public boolean unboundedMin;
      public boolean unboundedMax;
      public byte[] value;
   }

   public static Lex parseLex(byte[] arg) {
      if (arg.length == 0) {
         return null;
      }

      Lex lex = new Lex();
      if (arg.length == 1 && (arg[0] == (byte)'-')) {
         lex.unboundedMin = true;
         return lex;
      }
      if (arg.length == 1 && (arg[0] == (byte)'+')) {
         lex.unboundedMax = true;
         return lex;
      }

      lex.include = arg[0] == (byte) '[';
      if (lex.include || arg[0] == (byte)'(') {
         lex.value = Arrays.copyOfRange(arg, 1, arg.length);
      } else {
         // The value MUST start with '(' or '['
         return null;
      }
      return lex;
   }

   public static Score parseScore(byte[] arg) {
      try {
         Score score = new Score();
         if (isInf(arg, '-')) {
            score.unboundedMin = true;
            return score;
         }

         if (isInf(arg, '+')) {
            score.unboundedMax = true;
            return score;
         }

         if (arg[0] == ZSetCommonUtils.EXCLUDE) {
            score.value = ArgumentUtils.toDouble(arg, 1);
            score.include = false;
         } else {
            score.value = ArgumentUtils.toDouble(arg);
         }
         return score;
      } catch (NumberFormatException ex) {

      }
      return null;
   }

   /**
    * Transforms the resulting collection depending on the zrank options
    * - return scores
    * - limit results
    * @param scoredValues, scoresValues retrieved
    * @param withScores, return with scores
    * @return The Z operation response object to serialize in the RESP3 format.
    */
   public static ZOperationResponse response(Collection<ScoredValue<byte[]>> scoredValues, boolean withScores) {
      return new ZOperationResponse(scoredValues, withScores);
   }

   public record ZOperationResponse(Collection<ScoredValue<byte[]>> values, boolean withScores) implements JavaObjectSerializer<ZOperationResponse> {

      @Override
      public void accept(ZOperationResponse ignore, ByteBufPool alloc) {
         ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, values.size(), alloc);

         for (ScoredValue<byte[]> sv : values) {
            if (withScores) {
               ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, 2, alloc);
               Resp3Response.string(sv.getValue(), alloc);
               Resp3Response.doubles(sv.score(), alloc);
            } else {
               Resp3Response.string(sv.getValue(), alloc);
            }
         }
      }
   }
}
