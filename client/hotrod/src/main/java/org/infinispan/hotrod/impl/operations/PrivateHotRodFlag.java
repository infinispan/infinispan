package org.infinispan.hotrod.impl.operations;

public enum PrivateHotRodFlag {
   FORCE_RETURN_VALUE(0x0001);

   private final int flagInt;

   PrivateHotRodFlag(int flagInt) {
      this.flagInt = flagInt;
   }

   public int getFlagInt() {
      return flagInt;
   }
}
