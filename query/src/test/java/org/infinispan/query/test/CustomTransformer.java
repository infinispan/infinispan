package org.infinispan.query.test;

import org.infinispan.query.Transformer;

import java.util.StringTokenizer;

public class CustomTransformer implements Transformer {
   @Override
   public Object fromString(String s) {
      StringTokenizer strtok = new StringTokenizer(s, ",");
      int[] ints = new int[3];
      int i = 0;
      while (strtok.hasMoreTokens()) {
         String token = strtok.nextToken();
         String[] contents = token.split("=");
         ints[i++] = Integer.parseInt(contents[1]);
      }
      return new CustomKey(ints[0], ints[1], ints[2]);
   }

   @Override
   public String toString(Object customType) {
      CustomKey ck = (CustomKey) customType;
      return "i=" + ck.i + ",j=" + ck.j + ",k=" + ck.k;
   }
}
