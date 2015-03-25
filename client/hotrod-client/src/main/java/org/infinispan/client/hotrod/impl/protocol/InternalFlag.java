package org.infinispan.client.hotrod.impl.protocol;

/**
 * Flags which are internal to the protocol and shouldn't be exposed to users.
 */
public enum InternalFlag {
   /**
    * Indicates that lifespan and maxidle represent nanoseconds instead of seconds.
    */
   NANO_DURATIONS(0x0020);

   private int flagInt;

   InternalFlag(int flagInt) {
      this.flagInt = flagInt;
   }

   public int getFlagInt() {
      return flagInt;
   }

}
