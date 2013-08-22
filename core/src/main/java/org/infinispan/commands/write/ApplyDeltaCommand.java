package org.infinispan.commands.write;

import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaCompositeKey;
import org.infinispan.commands.Visitor;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;


/**
 * @author Vladimir Blagojevic
 * @since 5.1
 */
public class ApplyDeltaCommand extends AbstractDataWriteCommand {

   public static final int COMMAND_ID = 25;

   private Collection<Object> keys;
   private Delta delta;

   public ApplyDeltaCommand() {
   }

   public ApplyDeltaCommand(Object deltaAwareValueKey, Delta delta, Collection<Object> keys) {
      super(deltaAwareValueKey, EnumSet.of(Flag.DELTA_WRITE));

      if (keys == null || keys.isEmpty())
         throw new IllegalArgumentException("At least one key to be locked needs to be specified");

      this.keys = keys;
      this.delta = delta;
   }

   public Delta getDelta(){
      return delta;
   }

   /**
    * Performs an application of delta on a specified entry
    *
    * @param ctx invocation context
    * @return null
    */
   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      //nothing to do here
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "ApplyDeltaCommand[key=" + key + ", delta=" + delta + ", keys=" + keys+ ']';
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, delta, keys, Flag.copyWithoutRemotableFlags(flags)};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] args) {
      // TODO: Check duplicated in all commands? A better solution is needed.
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Unsupported command id:" + commandId);
      int i = 0;
      key = args[i++];
      delta = (Delta)args[i++];
      keys = (List<Object>) args[i++];
      flags = (Set<Flag>) args[i];
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitApplyDeltaCommand(ctx, this);
   }

   public Object[] getKeys() {
      return keys.toArray();
   }

   public Object[] getCompositeKeys() {
      DeltaCompositeKey[] compositeKeys = new DeltaCompositeKey[keys.size()];
      int i = 0;
      for (Object k : keys) {
         compositeKeys[i++] = new DeltaCompositeKey(key, k);
      }
      return compositeKeys;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      switch (status) {
         case FAILED:
         case INITIALIZING:
         case STOPPING:
         case TERMINATED:
            return true;
         default:
            return false;
         }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof ApplyDeltaCommand)) {
         return false;
      }
      if (!super.equals(o)) {
         return false;
      }

      ApplyDeltaCommand that = (ApplyDeltaCommand) o;
      return keys.equals(that.keys);
   }

   @Override
   public int hashCode() {
      return 31 * super.hashCode() + keys.hashCode();
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
   public boolean isIgnorePreviousValue() {
      return false;
   }

   @Override
   public void setIgnorePreviousValue(boolean ignorePreviousValue) {
   }
}
