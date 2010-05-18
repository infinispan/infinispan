package org.infinispan.client.hotrod;

/**
 * Defines all the flags available in hotrod that can influence the behavior of operations.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public enum Flag {

   /**
    * By default, previously existing values for Map operations are not returned. E.g. remoteCache.put(k,v) does not return
    * the previous value associated with k. Passing this flag overrides default behavior and previous existing values are
    * returned.
    */
   FORCE_RETURN_VALUE(0x0001);

   private int flagInt;

   Flag(int flagInt) {
      this.flagInt = flagInt;
   }

   public int getFlagInt() {
      return flagInt;
   }
}
