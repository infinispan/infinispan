package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.server.resp.Util;
import org.infinispan.server.resp.commands.ArgumentUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public final class ZSetCommonUtils {
   public static final byte[] WITHSCORES = "WITHSCORES".getBytes();
   public static byte EXCLUDE = ((byte)'(');

   private ZSetCommonUtils() {
   }

   public static boolean isWithScoresArg(byte[] arg) {
      return Util.isAsciiBytesEquals(arg, WITHSCORES);
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
    * @return byte[] list to be returned by the command
    */
   public static List<byte[]> mapResultsToArrayList(Collection<SortedSetBucket.ScoredValue<byte[]>> scoredValues, boolean withScores) {
      List<byte[]> elements = new ArrayList<>();
      Iterator<SortedSetBucket.ScoredValue<byte[]>> ite = scoredValues.iterator();
      while (ite.hasNext()) {
         SortedSetBucket.ScoredValue<byte[]> item = ite.next();
         elements.add(item.getValue());
         if (withScores) {
            elements.add(Double.toString(item.score()).getBytes(StandardCharsets.US_ASCII));
         }
      }
      return elements;
   }
}
