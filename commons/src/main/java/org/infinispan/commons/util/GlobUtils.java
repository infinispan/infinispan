package org.infinispan.commons.util;

/**
 * Utility functions for globs
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public final class GlobUtils {

   public static boolean isGlob(String s) {
      for(int i = 0; i < s.length(); i++) {
         char ch = s.charAt(i);
         if (ch == '*' || ch == '?') {
            return true;
         }
      }
      return false;
   }

   public static String globToRegex(String glob) {
      StringBuilder s = new StringBuilder();
      for(int i = 0; i < glob.length(); i++) {
         final char c = glob.charAt(i);
         switch(c) {
            case '*':
               s.append(".*");
               break;
            case '?':
               s.append('.');
               break;
            case '.':
               s.append("\\.");
               break;
            case '\\':
               s.append("\\\\");
               break;
            default:
               s.append(c);
         }
      }
      return s.toString();

   }
}
