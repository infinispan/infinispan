package org.infinispan.api;

import java.util.EnumSet;

import org.infinispan.api.common.CacheOptions;

/**
 * @since 14.0
 **/
public enum HotRodFlag implements CacheOptions.Flag<HotRodFlag> {
   SKIP_LOAD,
   SKIP_NOTIFICATION,
   FORCE_RETURN_VALUES;

   @Override
   public EnumSet<HotRodFlag> apply(EnumSet<HotRodFlag> flags) {
      if (flags == null) {
         flags = EnumSet.of(this);
      } else {
         flags.add(this);
      }
      return flags;
   }

   static HotRodFlag skipLoad() {
      return SKIP_LOAD;
   }

   static HotRodFlag skipNotification() {
      return SKIP_NOTIFICATION;
   }

   static HotRodFlag forceReturnValues() {
      return FORCE_RETURN_VALUES;
   }
}
