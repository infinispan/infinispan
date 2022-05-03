package org.infinispan.hotrod;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.Flags;

/**
 * @since 14.0
 **/
public class HotRodFlags implements Flags<HotRodFlag, HotRodFlags> {
   int flags;

   HotRodFlags() {}

   public static HotRodFlags of(HotRodFlag... flag) {
      HotRodFlags flags = new HotRodFlags();
      for(HotRodFlag f : flag) {
         flags.add(f);
      }
      return flags;
   }

   public static int toInt(CacheOptions.Impl options) {
      HotRodFlags flags = (HotRodFlags) options.rawFlags();
      return flags == null ? 0 : flags.toInt();
   }

   @Override
   public HotRodFlags add(HotRodFlag flag) {
      flags |= flag.getFlagInt();
      return this;
   }

   @Override
   public boolean contains(HotRodFlag flag) {
      return (flags & flag.getFlagInt()) != 0;
   }

   @Override
   public HotRodFlags addAll(Flags<HotRodFlag, HotRodFlags> flags) {
      HotRodFlags theFlags = (HotRodFlags) flags;
      this.flags |= theFlags.flags;
      return this;
   }

   public int toInt() {
      return flags;
   }
}
