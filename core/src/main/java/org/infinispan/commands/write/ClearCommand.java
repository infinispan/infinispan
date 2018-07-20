package org.infinispan.commands.write;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.notifications.cachelistener.CacheNotifier;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ClearCommand extends AbstractTopologyAffectedCommand implements WriteCommand {

   public static final byte COMMAND_ID = 5;
   private CacheNotifier<Object, Object> notifier;
   private DataContainer<?,?> dataContainer;

   public ClearCommand() {
   }

   public ClearCommand(CacheNotifier<Object, Object> notifier, DataContainer<?, ?> dataContainer, long flagsBitSet) {
      this.notifier = notifier;
      this.dataContainer = dataContainer;
      setFlagsBitSet(flagsBitSet);
   }

   public void init(CacheNotifier<Object, Object> notifier, DataContainer<?, ?> dataContainer) {
      this.notifier = notifier;
      this.dataContainer = dataContainer;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitClearCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      dataContainer.forEach(e ->
            notifier.notifyCacheEntryRemoved(e.getKey(), e.getValue(), e.getMetadata(), true, ctx, this));
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserObjectOutput output) throws IOException {
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
   }

   @Override
   public void readFrom(UserObjectInput input) throws IOException, ClassNotFoundException {
      setFlagsBitSet(input.readLong());
   }

   @Override
   public String toString() {
      return new StringBuilder()
         .append("ClearCommand{flags=")
         .append(printFlags())
         .append("}")
         .toString();
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
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
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
