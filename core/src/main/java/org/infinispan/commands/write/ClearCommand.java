package org.infinispan.commands.write;

import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.cachelistener.CacheNotifier;

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

   public ClearCommand(CacheNotifier<Object, Object> notifier, DataContainer<?,?> dataContainer, Set<Flag> flags) {
      this.notifier = notifier;
      this.dataContainer = dataContainer;
      this.flags = flags;
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
   public Object[] getParameters() {
      return new Object[]{Flag.copyWithoutRemotableFlags(flags)};
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalStateException("Invalid command id");
      if (parameters.length > 0) {
         this.flags = (Set<Flag>) parameters[0];
      }
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public String toString() {
      return new StringBuilder()
         .append("ClearCommand{flags=")
         .append(flags)
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
      return InfinispanCollections.emptySet();
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

}
