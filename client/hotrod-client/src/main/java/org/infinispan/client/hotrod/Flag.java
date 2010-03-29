package org.infinispan.client.hotrod;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public enum Flag {
   FORCE_RETURN_VALUE(0x0800);

   private int flagInt;

   Flag(int flagInt) {
      this.flagInt = flagInt;
   }

   public int getFlagInt() {
      return flagInt;
   }
}
