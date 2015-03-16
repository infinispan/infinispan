package org.infinispan.client.hotrod.impl.protocol;

/**
 * Flags which are internal to the protocol and shouldn't be exposed to users.
 */
public enum InternalFlag {
   /**
    * Indicates the the LIFESPAN_NANOS field will be present in the message.
    */
   LIFESPAN_NANOS(0x0020),
   /**
    * Indicates the the MAXIDLE_NANOS field will be present in the message.
    */
   MAXIDLE_NANOS(0x0040);

   private int flagInt;

   InternalFlag(int flagInt) {
      this.flagInt = flagInt;
   }

   public int getFlagInt() {
      return flagInt;
   }

}
