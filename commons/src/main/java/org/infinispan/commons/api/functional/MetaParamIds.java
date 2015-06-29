package org.infinispan.commons.api.functional;

/**
 * Meta parameter identifiers.
 *
 * @since 8.0
 */
public final class MetaParamIds {

   public static final int LIFESPAN_ID = 0;
   public static final int CREATED_ID = 1;
   public static final int MAX_IDLE_ID = 2;
   public static final int LAST_USED_ID = 3;
   public static final int ENTRY_VERSION_ID = 4;

   private MetaParamIds() {
      // Cannot be instantiated, it's just a holder class
   }

}
