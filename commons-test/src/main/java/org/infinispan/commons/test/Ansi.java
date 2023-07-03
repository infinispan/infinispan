package org.infinispan.commons.test;

/**
 * @since 15.0
 **/
public class Ansi {
   public static final String BLACK = "\u001b[30m";
   public static final String RED = "\u001b[31m";
   public static final String GREEN = "\u001b[32m";
   public static final String YELLOW = "\u001b[33m";
   public static final String BLUE = "\u001b[34m";
   public static final String PURPLE = "\u001b[35m";
   public static final String CYAN = "\u001b[36m";
   public static final String WHITE = "\u001b[37m";
   public static final String RESET = "\u001b[0m";

   public static final String[] DISTINCT_COLORS = {
         fg(63),
         fg(208),
         fg(213),
         fg(43),
         fg(99),
         fg(159),
         fg(202),
         fg(229)
   };

   public static final boolean useColor = !Boolean.getBoolean("ansi.strip");

   private Ansi() {
   }

   public static String fg(int color) {
      return "\u001b[38;5;" + color + "m";
   }
}
