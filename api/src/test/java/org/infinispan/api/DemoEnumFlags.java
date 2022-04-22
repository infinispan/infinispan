package org.infinispan.api;

import java.util.EnumSet;

import org.infinispan.api.common.Flags;

/**
 * @since 14.0
 **/
public class DemoEnumFlags implements Flags<DemoEnumFlag, DemoEnumFlags> {
   private final EnumSet<DemoEnumFlag> flags = EnumSet.noneOf(DemoEnumFlag.class);
   DemoEnumFlags() {}

   public static DemoEnumFlags of(DemoEnumFlag... flag) {
      DemoEnumFlags flags = new DemoEnumFlags();
      for(DemoEnumFlag f : flag) {
         flags.add(f);
      }
      return flags;
   }

   @Override
   public DemoEnumFlags add(DemoEnumFlag flag) {
      flags.add(flag);
      return this;
   }

   @Override
   public boolean contains(DemoEnumFlag flag) {
      return flags.contains(flag);
   }

   @Override
   public DemoEnumFlags addAll(Flags<DemoEnumFlag, DemoEnumFlags> flags) {
      DemoEnumFlags theFlags = (DemoEnumFlags) flags;
      this.flags.addAll(theFlags.flags);
      return this;
   }
}
