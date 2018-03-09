package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.Metadatas;
import org.infinispan.notifications.cachelistener.CacheNotifier;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public class ReplaceCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {
   public static final byte COMMAND_ID = 11;

   private Object oldValue;
   private Object newValue;
   private Metadata metadata;
   private CacheNotifier<Object, Object> notifier;
   private boolean successful = true;

   public ReplaceCommand() {
   }

   public ReplaceCommand(Object key, Object oldValue, Object newValue,
                         CacheNotifier notifier, Metadata metadata, long flagsBitSet,
                         CommandInvocationId commandInvocationId, InvocationManager invocationManager) {
      super(key, flagsBitSet, commandInvocationId, invocationManager);
      this.oldValue = oldValue;
      this.newValue = newValue;
      //noinspection unchecked
      this.notifier = notifier;
      this.metadata = metadata;
   }

   public void init(CacheNotifier notifier, InvocationManager invocationManager) {
      //noinspection unchecked
      this.notifier = notifier;
      this.invocationManager = invocationManager;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReplaceCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      //noinspection unchecked
      MVCCEntry<Object, Object> e = (MVCCEntry) ctx.lookupEntry(key);
      // We need the null check as in non-tx caches we don't always wrap the entry on the origin
      Object prevValue = e.getValue();
      Metadata prevMetadata = e.getMetadata();
      if ((oldValue == null && prevValue != null) || (oldValue != null && oldValue.equals(prevValue))) {
         e.setChanged(true);
         e.setValue(newValue);

         Object result = oldValue != null ? Boolean.TRUE : prevValue;
         recordInvocation(ctx, e, prevValue, prevMetadata, Metadatas.merged(prevMetadata, metadata));
         notifier.notifyCacheEntryModified(key, newValue, e.getMetadata(), prevValue, prevMetadata, true, ctx, this);
         return result;
      } else {
         successful = false;
         return oldValue != null ? Boolean.FALSE : prevValue;
      }
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      output.writeObject(oldValue);
      output.writeObject(newValue);
      output.writeObject(metadata);
      CommandInvocationId.writeTo(output, commandInvocationId);
      CommandInvocationId.writeTo(output, lastInvocationId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      setFlagsBitSet(input.readLong());
      oldValue = input.readObject();
      newValue = input.readObject();
      metadata = (Metadata) input.readObject();
      commandInvocationId = CommandInvocationId.readFrom(input);
      lastInvocationId = CommandInvocationId.readFrom(input);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      ReplaceCommand that = (ReplaceCommand) o;

      if (metadata != null ? !metadata.equals(that.metadata) : that.metadata != null) return false;
      if (newValue != null ? !newValue.equals(that.newValue) : that.newValue != null) return false;
      return oldValue != null ? oldValue.equals(that.oldValue) : that.oldValue == null;

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
      return true;
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
   public void fail() {
      successful = false;
   }

   @Override
   public final boolean isReturnValueExpected() {
     return true;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder()
            .append("ReplaceCommand{key=").append(toStr(key))
            .append(", oldValue=").append(toStr(oldValue))
            .append(", newValue=").append(toStr(newValue))
            .append(", metadata=").append(metadata)
            .append(", flags=")
            .append(printFlags())
            .append(", commandInvocationId=").append(CommandInvocationId.show(commandInvocationId))
            .append(", lastInvocationId=").append(lastInvocationId)
            .append(", successful=").append(successful)
            .append(", topologyId=").append(getTopologyId());
      return sb.append('}').toString();
   }
}
