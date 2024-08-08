package org.infinispan.embedded;

import org.infinispan.api.common.Flag;
import org.infinispan.api.common.Flags;

/**
 * Defines all the flags available in Embedded mode that can influence the behavior of operations.
 * <p/>
 * Available flags:
 * <ul>
 *    <li>{@link #DEFAULT_LIFESPAN}     This flag can either be used as a request flag during a put operation to mean
 *                                      that the default server lifespan should be applied or as a response flag meaning that
 *                                      the return entry has a default lifespan value</li>
 * </ul>
 *
 * @since 15.0
 */
public enum EmbeddedFlag implements Flag {
   TODO(0);

   private final int flagInt;

   EmbeddedFlag(int flagInt) {
      this.flagInt = flagInt;
   }

   public int getFlagInt() {
      return flagInt;
   }

   @Override
   public Flags<?, ?> add(Flags<?, ?> flags) {
      EmbeddedFlags userFlags = (EmbeddedFlags) flags;
      if (userFlags == null) {
         userFlags = EmbeddedFlags.of(this);
      } else {
         userFlags.add(this);
      }
      return userFlags;
   }
}
