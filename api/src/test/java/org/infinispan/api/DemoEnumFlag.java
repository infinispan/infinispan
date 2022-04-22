package org.infinispan.api;

import org.infinispan.api.common.Flag;
import org.infinispan.api.common.Flags;

/**
 * @since 14.0
 **/
public enum DemoEnumFlag implements Flag {
   SKIP_LOAD,
   SKIP_NOTIFICATION,
   FORCE_RETURN_VALUES;

   static DemoEnumFlag skipLoad() {
      return SKIP_LOAD;
   }

   static DemoEnumFlag skipNotification() {
      return SKIP_NOTIFICATION;
   }

   static DemoEnumFlag forceReturnValues() {
      return FORCE_RETURN_VALUES;
   }

   @Override
   public Flags<?, ?> add(Flags<?, ?> flags) {
      DemoEnumFlags demoFlags = (DemoEnumFlags) flags;
      if (demoFlags == null) {
         demoFlags = DemoEnumFlags.of(this);
      } else {
         demoFlags.add(this);
      }
      return demoFlags;
   }
}
