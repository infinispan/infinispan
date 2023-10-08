package org.infinispan.commands;

import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * Base class for those commands that can carry flags.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public abstract class AbstractFlagAffectedCommand implements FlagAffectedCommand {

   protected long flags;

   protected AbstractFlagAffectedCommand(long flags) {
      this.flags = flags;
   }

   @ProtoField(number = 1, name = "flags")
   public long getFlagsWithoutRemote() {
      return FlagBitSets.copyWithoutRemotableFlags(flags);
   }

   @Override
   public long getFlagsBitSet() {
      return flags;
   }

   @Override
   public void setFlagsBitSet(long bitSet) {
      this.flags = bitSet;
   }

   protected final boolean hasSameFlags(FlagAffectedCommand other) {
      return this.flags == other.getFlagsBitSet();
   }

   protected final String printFlags() {
      return EnumUtil.prettyPrintBitSet(flags, Flag.class);
   }
}
