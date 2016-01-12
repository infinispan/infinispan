package org.infinispan.commands.functional;

import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.FunctionalNotifier;
import org.infinispan.functional.impl.Params;
import org.infinispan.metadata.Metadata;

import java.util.Set;

abstract class AbstractWriteManyCommand<K, V> implements WriteCommand, ParamsCommand {

   boolean isForwarded = false;
   int topologyId = -1;
   Params params;

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   public boolean isForwarded() {
      return isForwarded;
   }

   public void setForwarded(boolean forwarded) {
      isForwarded = forwarded;
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      // No-op
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public Set<Flag> getFlags() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public Metadata getMetadata() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void setMetadata(Metadata metadata) {
      // TODO: Customise this generated block
   }

   @Override
   public void setFlags(Set<Flag> flags) {
      // TODO: Customise this generated block
   }

   @Override
   public void setFlags(Flag... flags) {
      // TODO: Customise this generated block
   }

   @Override
   public boolean hasFlag(Flag flag) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public long getFlagsBitSet() {
      return EnumUtil.EMPTY_BIT_SET;
   }

   @Override
   public void setFlagsBitSet(long bitSet) {
      // TODO: Customise this generated block
   }

   @Override
   public Params getParams() {
      return params;
   }

   public void setParams(Params params) {
      this.params = params;
   }


}
