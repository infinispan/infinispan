package org.infinispan.commons.configuration.io;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public enum NamingStrategy {
   IDENTITY {
      @Override
      public String convert(String s) {
         return s;
      }
   },
   CAMEL_CASE {
      @Override
      public String convert(String s) {
         StringBuilder b = new StringBuilder();
         for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if ((c == '-' || c == '_') && (i < s.length() - 1)) {
               b.append(Character.toUpperCase(s.charAt(++i)));
            } else {
               b.append(c);
            }
         }
         return b.toString();
      }
   },
   KEBAB_CASE {
      @Override
      public String convert(String s) {
         StringBuilder b = new StringBuilder();
         for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (i > 0 && isWordBoundary(c, s.charAt(i - 1))) {
               b.append('-');
               b.append(Character.toLowerCase(c));
            } else {
               b.append(c);
            }
         }
         return b.toString();
      }
   },
   SNAKE_CASE {
      @Override
      public String convert(String s) {
         StringBuilder b = new StringBuilder();
         for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (i > 0 && isWordBoundary(c, s.charAt(i - 1))) {
               b.append('_');
               b.append(Character.toLowerCase(c));
            } else {
               b.append(c);
            }
         }
         return b.toString();
      }
   };

   static boolean isWordBoundary(char ch1, char ch2) {
      return Character.isUpperCase(ch1) && (Character.isLowerCase(ch2) || Character.isDigit(ch2));
   }

   public abstract String convert(String s);
}
