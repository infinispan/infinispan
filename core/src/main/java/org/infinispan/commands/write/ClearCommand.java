package org.infinispan.commands.write;

import java.util.Collection;
import java.util.Collections;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CLEAR_COMMAND)
public class ClearCommand extends AbstractTopologyAffectedCommand implements WriteCommand {

   @ProtoFactory
   ClearCommand(long flagsWithoutRemote, int topologyId) {
      super(flagsWithoutRemote, topologyId);
   }

   public ClearCommand(long flagsBitSet) {
      super(flagsBitSet, -1);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitClearCommand(ctx, this);
   }

   @Override
   public String toString() {
      return "ClearCommand{flags=" + printFlags() + "}";
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
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      // Do nothing
   }

   @Override
   public Collection<?> getAffectedKeys() {
      return Collections.emptySet();
   }

   @Override
   public void fail() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CommandInvocationId getCommandInvocationId() {
      return null;
   }

   @Override
   public PrivateMetadata getInternalMetadata(Object key) {
      return null;
   }

   @Override
   public void setInternalMetadata(Object key, PrivateMetadata internalMetadata) {
      //no-op
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public LoadType loadType() {
      return LoadType.DONT_LOAD;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ClearCommand that = (ClearCommand) o;

      if (getTopologyId() != that.getTopologyId()) return false;
      return getFlagsBitSet() == that.getFlagsBitSet();

   }

   @Override
   public int hashCode() {
      int result = getTopologyId();
      long flags = getFlagsBitSet();
      result = 31 * result + (int) (flags ^ (flags >>> 32));
      return result;
   }
}
