package org.infinispan.commands.write;

import org.infinispan.commands.Visitor;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;


/**
 * @author Mircea.Markus@jboss.com
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */
public class RemoveCommand extends AbstractDataWriteCommand {
   private static final Log log = LogFactory.getLog(RemoveCommand.class);
   public static final byte COMMAND_ID = 10;
   protected CacheNotifier notifier;
   boolean successful = true;
   boolean nonExistent = false;
   private boolean ignorePreviousValue;
   private Equivalence valueEquivalence;

   /**
    * When not null, value indicates that the entry should only be removed if the key is mapped to this value. By the
    * time the RemoveCommand needs to be marshalled, the condition must have been true locally already, so there's no
    * need to marshall the value. *
    */
   protected transient Object value;

   public RemoveCommand(Object key, Object value, CacheNotifier notifier, Set<Flag> flags, Equivalence valueEquivalence) {
      super(key, flags);
      this.value = value;
      this.notifier = notifier;
      this.valueEquivalence = valueEquivalence;
   }

   public void init(CacheNotifier notifier, Configuration configuration) {
      this.notifier = notifier;
      this.valueEquivalence = configuration.dataContainer().valueEquivalence();
   }

   public RemoveCommand() {
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitRemoveCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      if (skipCommand(ctx)) {
         //ignore previous return value in tx mode is false when the command did not succeed during execution
         //in this case, we should ignore the command
         //the return value did not matter in remote context
         successful = false;
         return null;
      }
      CacheEntry e = ctx.lookupEntry(key);
      if (e == null || e.isNull() || e.isRemoved()) {
         nonExistent = true;
         log.trace("Nothing to remove since the entry is null or we have a null entry");
         if (value == null || ignorePreviousValue) {
            if (e != null) {
               e.setChanged(true);
               e.setRemoved(true);
            }
            return null;
         } else {
            successful = false;
            return false;
         }
      }

      if (!(e instanceof MVCCEntry)) ctx.putLookedUpEntry(key, null);

      if (!ignorePreviousValue && value != null && e.getValue() != null && !isValueEquals(e.getValue(), value)) {
         successful = false;
         return false;
      }

      if (this instanceof EvictCommand) {
         e.setEvicted(true);
      }

      return performRemove(e, ctx);
   }

   protected boolean skipCommand(InvocationContext ctx) {
      return ctx.isInTxScope() && !ctx.isOriginLocal() && !ignorePreviousValue;
   }

   protected void notify(InvocationContext ctx, Object value, boolean isPre) {
      notifier.notifyCacheEntryRemoved(key, value, value, isPre, ctx, this);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof RemoveCommand)) {
         return false;
      }
      if (!super.equals(o)) {
         return false;
      }

      RemoveCommand that = (RemoveCommand) o;

      if (value != null ? !value.equals(that.value) : that.value != null) {
         return false;
      }

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
   }


   @Override
   public String toString() {
      return new StringBuilder()
         .append("RemoveCommand{key=")
         .append(key)
         .append(", value=").append(value)
         .append(", flags=").append(flags)
         .append(", ignorePreviousValue=").append(ignorePreviousValue)
         .append("}")
         .toString();
   }

   @Override
   public boolean isSuccessful() {
      return successful;
   }

   @Override
   public boolean isConditional() {
      return value != null;
   }

   public boolean isNonExistent() {
      return nonExistent;
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalStateException("Invalid method id");
      key = parameters[0];
      value = parameters[1];
      flags = (Set<Flag>) parameters[2];
      ignorePreviousValue = (Boolean) parameters[3];
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, value, Flag.copyWithoutRemotableFlags(flags), ignorePreviousValue};
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
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   public Object getValue() {
      return value;
   }

   public void setValue(Object value) {
      this.value = value;
   }

   @Override
   public final boolean isReturnValueExpected() {
      //SKIP_RETURN_VALUE ignored for conditional remove
      return super.isReturnValueExpected() || isConditional();
   }

   private Object performRemove(CacheEntry e, InvocationContext ctx) {
      final Object removedValue = e.getValue();
      notify(ctx, removedValue, true);

      e.setRemoved(true);
      e.setValid(false);
      e.setChanged(true);

      // If ignorePreviousValue == true, the old value is no longer relevant
      if (!ignorePreviousValue) {
         return value == null ? removedValue : true;
      } else {
         return value == null ? value : true;
      }
   }

   @SuppressWarnings("unchecked")
   private boolean isValueEquals(Object oldValue, Object newValue) {
      if (valueEquivalence != null) {
         return valueEquivalence.equals(oldValue, newValue);
      }
      return oldValue.equals(newValue);
   }
}
