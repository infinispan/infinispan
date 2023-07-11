package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.ArgumentUtils;

import java.util.Arrays;

final class SortedSetArgumentsUtils {
   public static byte EXCLUDE = ((byte)'(');
   private SortedSetArgumentsUtils() {
   }

   private static boolean isInf(byte[] arg, char sign) {
      if (arg.length != 4)
         return false;

      return arg[0] == (byte) sign && arg[1] == (byte) 'i' && arg[2] == (byte) 'n' && arg[3] == (byte) 'f';
   }

   public static class Score {
      public boolean unboundedMin;
      public boolean unboundedMax;
      boolean include = true;
      Double value;
   }

   public static class Lex {
      boolean include;
      boolean unboundedMin;
      boolean unboundedMax;
      byte[] value;
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

         if (arg[0] == SortedSetArgumentsUtils.EXCLUDE) {
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

}
