package org.infinispan.functional;

import org.infinispan.commons.util.Experimental;

/**
 * Parameter identifiers.
 *
 * @since 8.0
 */
@Experimental
public final class ParamIds {

   public static final int PERSISTENCE_MODE_ID = 0;
   public static final int LOCKING_MODE_ID = 1;
   public static final int EXECUTION_MODE_ID = 2;

   private ParamIds() {
      // Cannot be instantiated, it's just a holder class
   }

}
