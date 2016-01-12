package org.infinispan.commands.write;

import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.cachelistener.CacheNotifier;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ClearCommand extends AbstractFlagAffectedCommand implements WriteCommand {
   
   public static final byte COMMAND_ID = 5;
   private CacheNotifier<Object, Object> notifier;
   private DataContainer<?,?> dataContainer;

   public ClearCommand() {
   }

   public ClearCommand(CacheNotifier<Object, Object> notifier, DataContainer<?,?> dataContainer, long flagsBitSet) {
      this.notifier = notifier;
      this.dataContainer = dataContainer;
      setFlagsBitSet(flagsBitSet);
   }

   public void init(CacheNotifier<Object, Object> notifier, DataContainer<?,?> dataContainer) {
      this.notifier = notifier;
      this.dataContainer = dataContainer;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitClearCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      for (CacheEntry e : dataContainer.entrySet()) {
         notifier.notifyCacheEntryRemoved(e.getKey(), e.getValue(), e.getMetadata(), true, ctx, this);
      }
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeLong(Flag.copyWithoutRemotableFlags(getFlagsBitSet()));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      setFlagsBitSet(input.readLong());
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
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
   public Set<Object> getAffectedKeys() {
      return Collections.emptySet();
   }

   @Override
   public void updateStatusFromRemoteResponse(Object remoteResponse) {
      // Do nothing
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
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   @Override
   public boolean readsExistingValues() {
      return false;
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
