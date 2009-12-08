package org.infinispan.query.test;

import org.infinispan.query.Transformer;

/**
 * Custom class that implements {@link org.infinispan.query.Transformer}
 *
 * @author Navin Surtani
 */


public class CustomTransformer implements Transformer {   


   @Override
   public Object fromString(String s) {
      // I know that the custom type is a CustomKey.
      // The string will be of the format of "aNumber=" + aNumber + ";name=" + name
      // For test purposes we can create a new instance of this class with the same values.

      int indexOfFirstEquals = s.indexOf("=");
      int indexOfSemiColon = s.indexOf(";");
      String aNumber = s.substring(indexOfFirstEquals, indexOfSemiColon);

      // We know that this index will be the first one after indexOfFirstEquals
      int indexOfSecondEquals = s.indexOf("=", indexOfFirstEquals);
      String name = s.substring(indexOfSecondEquals);

      return new CustomKey(name, Integer.parseInt(aNumber));
   }

   @Override
   public String toString(Object customType) {
      return customType.toString();
   }
}
