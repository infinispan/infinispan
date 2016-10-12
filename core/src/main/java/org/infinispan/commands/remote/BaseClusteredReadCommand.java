package org.infinispan.commands.remote;

import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.infinispan.util.ByteString;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class BaseClusteredReadCommand extends BaseRpcCommand {
   private long flags;

   protected BaseClusteredReadCommand(ByteString cacheName, long flagBitSet) {
      super(cacheName);
      this.flags = flagBitSet;
   }

   public long getFlagsBitSet() {
      return flags;
   }

   public void setFlagsBitSet(long bitSet) {
      flags = bitSet;
   }

   protected final String printFlags() {
      return EnumUtil.prettyPrintBitSet(flags, Flag.class);
   }

   public boolean hasFlag(Flag flag) {
      return EnumUtil.hasEnum(getFlagsBitSet(), flag);
   }
}
