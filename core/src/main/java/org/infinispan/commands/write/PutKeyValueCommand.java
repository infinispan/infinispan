package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.atomic.CopyableDeltaAware;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.Metadatas;
import org.infinispan.notifications.cachelistener.CacheNotifier;

/**
 * Implements functionality defined by {@link org.infinispan.Cache#put(Object, Object)}
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PutKeyValueCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {

   public static final byte COMMAND_ID = 8;

   private Object value;
   private boolean putIfAbsent;
   private CacheNotifier<Object, Object> notifier;
   private boolean successful = true;
   private Metadata metadata;

   public PutKeyValueCommand() {
   }

   public PutKeyValueCommand(Object key, Object value, boolean putIfAbsent,
                             CacheNotifier notifier, Metadata metadata, long flagsBitSet,
                             CommandInvocationId commandInvocationId, Object providedResult) {
      super(key, flagsBitSet, commandInvocationId, providedResult);
      this.value = value;
      this.putIfAbsent = putIfAbsent;
      //noinspection unchecked
      this.notifier = notifier;
      this.metadata = metadata;
      this.providedResult = providedResult;

      if (value instanceof DeltaAware) {
         addFlags(FlagBitSets.DELTA_WRITE);
      }
   }

   public void init(CacheNotifier notifier) {
      //noinspection unchecked
      this.notifier = notifier;
   }

   public Object getValue() {
      return value;
   }

   public void setValue(Object value) {
      this.value = value;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPutKeyValueCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      if (hasAnyFlag(FlagBitSets.DELTA_WRITE)) {
         return LoadType.OWNER;
      } else if (isConditional() || !hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES)) {
         return LoadType.PRIMARY;
      } else {
         return LoadType.DONT_LOAD;
      }
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      //noinspection unchecked
      MVCCEntry<Object, Object> e = (MVCCEntry) ctx.lookupEntry(key);

      if (e == null) {
         throw new IllegalStateException("Not wrapped");
      }

      Object prevValue = e.getValue();
      if (!hasAnyFlag(FlagBitSets.DISABLE_CONDITION) && putIfAbsent && prevValue != null) {
         successful = false;
         return prevValue;
      }

      return performPut(e, ctx);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(value);
      output.writeObject(metadata);
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      output.writeBoolean(putIfAbsent);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      value = input.readObject();
      metadata = (Metadata) input.readObject();
      commandInvocationId = CommandInvocationId.readFrom(input);
      setFlagsBitSet(input.readLong());
      putIfAbsent = input.readBoolean();
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   public boolean isPutIfAbsent() {
      return putIfAbsent;
   }

   public void setPutIfAbsent(boolean putIfAbsent) {
      this.putIfAbsent = putIfAbsent;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      PutKeyValueCommand that = (PutKeyValueCommand) o;

      if (putIfAbsent != that.putIfAbsent) return false;
      if (value != null ? !value.equals(that.value) : that.value != null) return false;
      return metadata != null ? metadata.equals(that.metadata) : that.metadata == null;

   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + (putIfAbsent ? 1 : 0);
      result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder()
            .append("PutKeyValueCommand{key=").append(toStr(key))
            .append(", value=").append(toStr(value))
            .append(", flags=").append(printFlags())
            .append(", commandInvocationId=").append(CommandInvocationId.show(commandInvocationId))
            .append(", putIfAbsent=").append(putIfAbsent)
            .append(", metadata=").append(metadata)
            .append(", successful=").append(successful)
            .append(", topologyId=").append(getTopologyId());
      if (hasAnyFlag(FlagBitSets.PROVIDED_RESULT)) {
         sb.append(", result=").append(providedResult);
      }
      return sb.append("}").toString();
   }

   @Override
   public boolean isSuccessful() {
      return successful;
   }

   @Override
   public boolean isConditional() {
      return putIfAbsent;
   }

   @Override
   public void initBackupWriteRpcCommand(BackupWriteRpcCommand command) {
      command.setWrite(commandInvocationId, key, value, metadata, getFlagsBitSet(), getTopologyId());
   }

   @Override
   public void fail() {
      successful = false;
   }

   @Override
   public boolean isReturnValueExpected() {
      return isConditional() || super.isReturnValueExpected();
   }

   private Object performPut(MVCCEntry<Object, Object> e, InvocationContext ctx) {
      Object entryValue = e.getValue();
      Object o;

      if (value instanceof Delta) {
         // magic
         Delta dv = (Delta) value;
         if (e.isRemoved()) {
            e.setExpired(false);
            e.setRemoved(false);
            e.setCreated(true);
            e.setValid(true);
            e.setValue(dv.merge(null));
         } else {
            DeltaAware toMergeWith = null;
            if (entryValue instanceof CopyableDeltaAware) {
               toMergeWith = ((CopyableDeltaAware) entryValue).copy();
            } else if (entryValue instanceof DeltaAware) {
               toMergeWith = (DeltaAware) entryValue;
            }
            e.setValue(dv.merge(toMergeWith));
         }
         o = entryValue;
      } else {
         o = e.setValue(value);
         if (e.isRemoved()) {
            e.setCreated(true);
            e.setExpired(false);
            e.setRemoved(false);
            e.setValid(true);
            o = null;
         }
      }
      e.setChanged(true);

      Object result = hasAnyFlag(FlagBitSets.PROVIDED_RESULT) ? providedResult : o;
      if (hasAnyFlag(FlagBitSets.WITH_INVOCATION_RECORDS)) {
         e.setMetadata(metadata);
      } else {
         recordInvocation(e, result, Metadatas.merged(e.getMetadata(), metadata));
      }

      if (e.isCreated()) {
         notifier.notifyCacheEntryCreated(key, value, metadata, true, ctx, this);
      } else {
         notifier.notifyCacheEntryModified(key, value, metadata, entryValue, e.getMetadata(), true, ctx, this);
      }

      return result;
   }
}
