package org.infinispan.hotrod;

import org.infinispan.api.common.Flag;
import org.infinispan.api.common.Flags;

/**
 * Defines all the flags available in the Hot Rod client that can influence the behavior of operations.
 * <p />
 * Available flags:
 * <ul>
 *    <li>{@link #DEFAULT_LIFESPAN}     This flag can either be used as a request flag during a put operation to mean
 *                                      that the default server lifespan should be applied or as a response flag meaning that
 *                                      the return entry has a default lifespan value</li>
 *    <li>{@link #DEFAULT_MAXIDLE}      This flag can either be used as a request flag during a put operation to mean
 *                                      that the default server maxIdle should be applied or as a response flag meaning that
 *                                      the return entry has a default maxIdle value</li>
 *    <li>{@link #SKIP_CACHE_LOAD}      Skips loading an entry from any configured
 *                                      cache loaders</li>
 *    <li>{@link #SKIP_INDEXING}        Used by the Query module only, it will prevent the indexes to be updated as a result
 *                                      of the current operations.
 *    <li>{@link #SKIP_LISTENER_NOTIFICATION}   Used when an operation wants to skip notifications to the registered listeners
 * </ul>
 *
 * @since 14.0
 */
public enum HotRodFlag implements Flag {

   /**
    * This flag can either be used as a request flag during a put operation to mean that the default
    * server lifespan should be applied or as a response flag meaning that the return entry has a
    * default lifespan value
    */
   DEFAULT_LIFESPAN(0x0002),
   /**
    * This flag can either be used as a request flag during a put operation to mean that the default
    * server maxIdle should be applied or as a response flag meaning that the return entry has a
    * default maxIdle value
    */
   DEFAULT_MAXIDLE(0x0004),
   /**
    * Skips loading an entry from any configured cache loaders
    */
   SKIP_CACHE_LOAD(0x0008),
   /**
    * Used by the Query module only, it will prevent the indexes to be updated as a result of the current operations.
    */
   SKIP_INDEXING(0x0010),
   /**
    * It will skip client listeners to be notified.
    */
   SKIP_LISTENER_NOTIFICATION(0x0020)
   ;

   private final int flagInt;

   HotRodFlag(int flagInt) {
      this.flagInt = flagInt;
   }

   public int getFlagInt() {
      return flagInt;
   }

   @Override
   public Flags<?, ?> add(Flags<?, ?> flags) {
      HotRodFlags userFlags = (HotRodFlags) flags;
      if (userFlags == null) {
         userFlags = HotRodFlags.of(this);
      } else {
         userFlags.add(this);
      }
      return userFlags;
   }
}
