package org.infinispan.embedded;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.Flags;

/**
 * @since 15.0
 **/
public class EmbeddedFlags implements Flags<EmbeddedFlag, EmbeddedFlags> {
   int flags;

   EmbeddedFlags() {
   }

   public static EmbeddedFlags of(EmbeddedFlag... flag) {
      EmbeddedFlags flags = new EmbeddedFlags();
      for (EmbeddedFlag f : flag) {
         flags.add(f);
      }
      return flags;
   }

   public static int toInt(CacheOptions.Impl options) {
      EmbeddedFlags flags = (EmbeddedFlags) options.rawFlags();
      return flags == null ? 0 : flags.toInt();
   }

   @Override
   public EmbeddedFlags add(EmbeddedFlag flag) {
      flags |= flag.getFlagInt();
      return this;
   }

   @Override
   public boolean contains(EmbeddedFlag flag) {
      return (flags & flag.getFlagInt()) != 0;
   }

   @Override
   public EmbeddedFlags addAll(Flags<EmbeddedFlag, EmbeddedFlags> flags) {
      EmbeddedFlags theFlags = (EmbeddedFlags) flags;
      this.flags |= theFlags.flags;
      return this;
   }

   public int toInt() {
      return flags;
   }
}
