package org.infinispan.client.hotrod;

import java.util.Map;

/**
 * Defines all the flags available in the Hot Rod client that can influence the behavior of operations.
 * <p />
 * Available flags:
 * <ul>
 *    <li>{@link #FORCE_RETURN_VALUE} - By default, previously existing values for {@link Map} operations are not
 *                                      returned. E.g. {@link RemoteCache#put(Object, Object)} does <i>not</i> return
 *                                      the previous value associated with the key.  By applying this flag, this default
 *                                      behavior is overridden for the scope of a single invocation, and the previous
 *                                      existing value is returned.</li>
 * </ul>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public enum Flag {

   /**
    * By default, previously existing values for {@link Map} operations are not returned. E.g. {@link RemoteCache#put(Object, Object)}
    * does <i>not</i> return the previous value associated with the key.
    * <p />
    * By applying this flag, this default behavior is overridden for the scope of a single invocation, and the previous
    * existing value is returned.
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
