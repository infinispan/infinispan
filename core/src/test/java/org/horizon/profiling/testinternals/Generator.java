package org.horizon.profiling.testinternals;

//import org.horizon.tree.Fqn;

import java.util.List;
import java.util.Random;

public class Generator {
   private static final Random r = new Random();

   public static String getRandomString() {
      StringBuilder sb = new StringBuilder();
      int len = r.nextInt(10);

      for (int i = 0; i < len; i++) {
         sb.append((char) (63 + r.nextInt(26)));
      }
      return sb.toString();
   }

   public static <T> T getRandomElement(List<T> list) {
      return list.get(r.nextInt(list.size()));
   }

   public static Object createRandomKey() {
      return Integer.toHexString(r.nextInt(Integer.MAX_VALUE));
   }
}
