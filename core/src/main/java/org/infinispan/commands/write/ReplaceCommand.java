package org.infinispan.commands.write;

import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;

import java.util.Set;

import static org.infinispan.commons.util.Util.toStr;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public class ReplaceCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {
   public static final byte COMMAND_ID = 11;

   Object oldValue;
   Object newValue;
   Metadata metadata;
   private CacheNotifier notifier;
   boolean successful = true;

   boolean ignorePreviousValue;
   private Equivalence valueEquivalence;

   public ReplaceCommand() {
   }

   public ReplaceCommand(Object key, Object oldValue, Object newValue,
         CacheNotifier notifier, Metadata metadata, Set<Flag> flags, Equivalence valueEquivalence) {
      super(key, flags);
      this.oldValue = oldValue;
      this.newValue = newValue;
      this.notifier = notifier;
      this.metadata = metadata;
      this.valueEquivalence = valueEquivalence;
   }
   
   public void init(CacheNotifier notifier, Configuration cfg) {
      this.notifier = notifier;
      this.valueEquivalence = cfg.dataContainer().valueEquivalence();
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReplaceCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      final boolean local = ctx.isOriginLocal();
      if (ctx.isInTxScope() && !local && !ignorePreviousValue) {
         //ignore previous return value in tx mode is false when the command did not succeed during execution
         //in this case, we should ignore the command
         //the return value did not matter in remote context
         successful = false;
         return null;
      }
      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
      if (e != null) {
         if (local) {
            //ISPN-514
            if (e.isNull() || e.getValue() == null || e.isRemoved()) {
               return returnValue(null, false, ctx);
            }
         }

         // isConditional is false when ignorePreviousValue is true
         if (oldValue == null || ignorePreviousValue || isValueEquals(oldValue, e.getValue())) {
            e.setChanged(true);
            Object old = e.setValue(newValue);
            // TODO if (newValue.equals(old) returnValue(null, true, ctx);
            // If ignorePreviousValue == true, the old value is no longer relevant
            if (!ignorePreviousValue) {
               return returnValue(old, true, ctx);
            } else {
               return returnValue(oldValue, true, ctx);
            }
         }
      }

      return returnValue(null, false, ctx);
   }

   @SuppressWarnings("unchecked")
   private boolean isValueEquals(Object oldValue, Object newValue) {
      if (valueEquivalence != null)
         return valueEquivalence.equals(oldValue, newValue);

      return oldValue.equals(newValue);
   }

   private Object returnValue(Object beingReplaced, boolean successful, 
         InvocationContext ctx) {
      this.successful = successful;
      
      Object previousValue = oldValue == null ? beingReplaced : oldValue;

      if (successful) {
         notifier.notifyCacheEntryModified(
               key, previousValue, previousValue == null, true, ctx, this);
      }

      if (oldValue == null) {
         return beingReplaced;
      } else {
         return successful;
      }
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, oldValue, newValue, metadata, ignorePreviousValue,
                          Flag.copyWithoutRemotableFlags(flags)};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalArgumentException("Invalid method name");
      key = parameters[0];
      oldValue = parameters[1];
      newValue = parameters[2];
      metadata = (Metadata) parameters[3];
      ignorePreviousValue = (Boolean) parameters[4];
      flags = (Set<Flag>) parameters[5];
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      ReplaceCommand that = (ReplaceCommand) o;

      if (metadata != null ? !metadata.equals(that.metadata) : that.metadata != null) return false;
      if (newValue != null ? !newValue.equals(that.newValue) : that.newValue != null) return false;
      if (oldValue != null ? !oldValue.equals(that.oldValue) : that.oldValue != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
      result = 31 * result + (newValue != null ? newValue.hashCode() : 0);
      result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
      return result;
   }

   @Override
   public boolean isSuccessful() {
      return successful;
   }

   @Override
   public boolean isConditional() {
      return oldValue != null;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   public Object getOldValue() {
      return oldValue;
   }

   public void setOldValue(Object oldValue) {
      this.oldValue = oldValue;
   }

   public Object getNewValue() {
      return newValue;
   }

   public void setNewValue(Object newValue) {
      this.newValue = newValue;
   }

   @Override
   public boolean isIgnorePreviousValue() {
      return ignorePreviousValue;
   }

   @Override
   public void setIgnorePreviousValue(boolean ignorePreviousValue) {
      this.ignorePreviousValue = ignorePreviousValue;
   }

   @Override
   public final boolean isReturnValueExpected() {
     //SKIP_RETURN_VALUE ignored for conditional replace
     return super.isReturnValueExpected() || isConditional();
   }

   @Override
   public String toString() {
      return "ReplaceCommand{" +
            "key=" + toStr(key) +
            ", oldValue=" + toStr(oldValue) +
            ", newValue=" + toStr(newValue) +
            ", metadata=" + metadata +
            ", flags=" + flags +
            ", successful=" + successful +
            ", ignorePreviousValue=" + ignorePreviousValue +
            '}';
   }
}
