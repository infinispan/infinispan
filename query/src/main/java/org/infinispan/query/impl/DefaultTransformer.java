package org.infinispan.query.impl;

import java.io.Serializable;

import org.infinispan.commons.util.Base64;
import org.infinispan.query.Transformer;

/**
 * Warning, slow as a dog, uses serialization to get a byte representation of a class.  Implement your own!
 *
 * Repeat. It is HIGHLY RECOMMENDED THAT YOU PROVIDE YOUR OWN IMPLEMENTATION OF {@link org.infinispan.query.Transformer}
 *
 * @author Navin Surtani
 */
public class DefaultTransformer implements Transformer {

   @Override
   public Object fromString(String s) {
      return Base64.decodeToObject(s);
   }

   @Override
   public String toString(Object customType) {
      if (customType instanceof Serializable) {
         return Base64.encodeObject((Serializable) customType);
      } else {
         throw new IllegalArgumentException("Expected " + customType.getClass() + " to be Serializable!");
      }
   }

}
