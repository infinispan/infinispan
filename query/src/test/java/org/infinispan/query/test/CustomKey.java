package org.infinispan.query.test;

import org.infinispan.query.Transformable;

/**
 * Test class used as a transformable. Used to test the code in {@link org.infinispan.query.backend.KeyTransformationHandler}
 * through the {@link org.infinispan.query.backend.KeyTransformationHandlerTest}
 *
 * @author Navin Surtani
 */

@Transformable(transformer = CustomTransformer.class)
public class CustomKey {

   private String name;
   private int aNumber;

   public CustomKey(String name, int aNumber){
      this.name = name;
      this.aNumber = aNumber;
   }

   public String getName(){
      return name;
   }

   public int getANumber(){
      return aNumber;
   }

   @Override
   public String toString(){
      return "aNumber=" + aNumber + ";name=" + name;
   }
}
