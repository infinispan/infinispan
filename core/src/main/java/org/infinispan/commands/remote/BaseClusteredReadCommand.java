package org.infinispan.commands.remote;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.ByteString;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class BaseClusteredReadCommand extends BaseRpcCommand implements TopologyAffectedCommand {
   protected int topologyId;
   protected long flags;

   protected BaseClusteredReadCommand(ByteString cacheName, int topologyId, long flagBitSet) {
      super(cacheName);
      this.topologyId = topologyId;
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

   public boolean hasAnyFlag(long flagBitSet) {
      return EnumUtil.containsAny(getFlagsBitSet(), flagBitSet);
   }

   @Override
   @ProtoField(2)
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @ProtoField(number = 3, name = "flags")
   long getFlagsWithoutRemote() {
      return FlagBitSets.copyWithoutRemotableFlags(flags);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
