package org.infinispan.commons.jdkspecific;

import org.infinispan.commons.spi.ScalarVectors;

/**
 * Factory for {@link Vectors}.
 */
public class Vectors {
   public static org.infinispan.commons.spi.Vectors getInstance() {
      return ScalarVectors.INSTANCE;
   }
}
