package org.infinispan.query.test;

import org.infinispan.query.Transformer;

public class CustomKey3Transformer implements Transformer {

   @Override
   public Object fromString(String s) {
      return new CustomKey3(s);
   }

   @Override
   public String toString(Object customType) {
      CustomKey3 key = (CustomKey3) customType;
      return key.getStr();
   }
}
