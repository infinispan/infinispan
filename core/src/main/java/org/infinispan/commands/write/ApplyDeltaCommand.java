package org.infinispan.commands.write;

import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaCompositeKey;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.DeltaAwareCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
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

   public ApplyDeltaCommand(Object deltaAwareValueKey, Delta delta, Collection<Object> keys, CommandInvocationId commandInvocationId) {
      super(deltaAwareValueKey, EnumSet.of(Flag.DELTA_WRITE), commandInvocationId);

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
      CacheEntry contextEntry = ctx.lookupEntry(key);
      if (contextEntry instanceof DeltaAwareCacheEntry) {
         DeltaAwareCacheEntry deltaAwareCacheEntry = (DeltaAwareCacheEntry) contextEntry;
         deltaAwareCacheEntry.appendDelta(delta);
      }
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
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(delta);
      MarshallUtil.marshallCollection(keys, output);
      output.writeObject(Flag.copyWithoutRemotableFlags(flags));
      output.writeObject(commandInvocationId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      delta = (Delta) input.readObject();
      keys = MarshallUtil.unmarshallCollection(input, ArrayList::new);
      flags = (Set<Flag>) input.readObject();
      commandInvocationId = (CommandInvocationId) input.readObject();
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
   public boolean readsExistingValues() {
      return true;
   }

   @Override
   public boolean alwaysReadsExistingValues() {
      return true;
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
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      // Do nothing
   }

   @Override
   public void updateStatusFromRemoteResponse(Object remoteResponse) {
      // Do nothing
   }
}
