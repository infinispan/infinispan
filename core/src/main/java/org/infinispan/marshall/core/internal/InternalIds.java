package org.infinispan.marshall.core.internal;

final class InternalIds {

   public static final int PRIMITIVE         = 0x00; // primitives
   public static final int NON_PRIMITIVE     = 0x01; // internal and user externalizers
   public static final int EXTERNAL          = 0x02; // external

   private InternalIds() {
   }

}
