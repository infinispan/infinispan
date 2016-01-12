package org.infinispan.commands.remote;

import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class LocalFlagAffectedRpcCommand extends BaseRpcCommand implements LocalFlagAffectedCommand {
   private long flags;

   protected LocalFlagAffectedRpcCommand(String cacheName, long flagBitSet) {
      super(cacheName);
      this.flags = flagBitSet;
   }

   @Override
   public long getFlagsBitSet() {
      return flags;
   }

   @Override
   public void setFlagsBitSet(long bitSet) {
      flags = bitSet;
   }

   protected final String printFlags() {
      return EnumUtil.prettyPrintBitSet(flags, Flag.class);
   }
}
