package org.infinispan.commons.jdkspecific;

import org.infinispan.commons.spi.ScalarVectors;

/**
 * Factory for {@link org.infinispan.commons.spi.Vectors}.
 */
public class Vectors {
   private static final org.infinispan.commons.spi.Vectors INSTANCE;

   static {
      org.infinispan.commons.spi.Vectors v;
      try {
         v = new VectorizedVectors();
      } catch (Throwable t) {
         v = ScalarVectors.INSTANCE;
      }
      INSTANCE = v;
   }

   public static org.infinispan.commons.spi.Vectors getInstance() {
      return INSTANCE;
   }
}
