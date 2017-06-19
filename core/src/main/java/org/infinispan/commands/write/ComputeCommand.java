package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.function.BiFunction;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.Metadatas;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.UserRaisedFunctionalException;

public class ComputeCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {

   public static final int COMMAND_ID = 68;

   private BiFunction remappingBiFunction;
   private Metadata metadata;
   private CacheNotifier<Object, Object> notifier;
   private boolean computeIfPresent;
   private boolean successful;

   public ComputeCommand() {
   }

   public ComputeCommand(Object key,
                         BiFunction remappingBiFunction,
                         boolean computeIfPresent,
                         long flagsBitSet,
                         CommandInvocationId commandInvocationId,
                         Metadata metadata,
                         CacheNotifier notifier,
                         ComponentRegistry componentRegistry) {

      super(key, flagsBitSet, commandInvocationId);
      this.remappingBiFunction = remappingBiFunction;
      this.computeIfPresent = computeIfPresent;
      this.metadata = metadata;
      this.notifier = notifier;
      componentRegistry.wireDependencies(this.remappingBiFunction);
   }

   public boolean isComputeIfPresent() {
      return computeIfPresent;
   }

   public void setComputeIfPresent(boolean computeIfPresent) {
      this.computeIfPresent = computeIfPresent;
   }

   public void init(CacheNotifier notifier, ComponentRegistry componentRegistry) {
      //noinspection unchecked
      this.notifier = notifier;
      componentRegistry.wireDependencies(remappingBiFunction);
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   @Override
   public boolean isSuccessful() {
      return successful;
   }

   @Override
   public boolean isConditional() {
      return computeIfPresent;
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      //implementation not needed
   }

   @Override
   public void fail() {
      successful = false;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      MVCCEntry<Object, Object> e = (MVCCEntry) ctx.lookupEntry(key);

      if (e == null) {
         throw new IllegalStateException("Not wrapped");
      }

      Object oldValue = e.getValue();
      Object newValue;


      if(computeIfPresent && oldValue == null) {
         successful = true;
         return oldValue;
      }

      try {
         newValue = remappingBiFunction.apply(key, oldValue);
      } catch (RuntimeException ex) {
         throw new UserRaisedFunctionalException(ex);
      }

      if (oldValue == null && newValue == null) {
         successful = true;
         return null;
      }

      Object computeResult = newValue;

      if (oldValue != null) {
         // The key already has a value
         if (newValue != null) {
            //replace with the new value if there is a modification on the value
            notifier.notifyCacheEntryModified(key, newValue, metadata, oldValue, e.getMetadata(), true, ctx, this);
            e.setChanged(true);
            e.setValue(newValue);
            Metadatas.updateMetadata(e, metadata);
         } else {
            // remove when new value is null
            notifier.notifyCacheEntryRemoved(key, oldValue, e.getMetadata(), true, ctx, this);
            e.setRemoved(true);
            e.setValid(false);
            e.setChanged(true);
            e.setValue(null);
         }
      } else {
         // put if not present
         notifier.notifyCacheEntryCreated(key, newValue, metadata, true, ctx, this);
         e.setValue(newValue);
         e.setChanged(true);
         Metadatas.updateMetadata(e, metadata);
         if (e.isRemoved()) {
            e.setCreated(true);
            e.setExpired(false);
            e.setRemoved(false);
            e.setValid(true);
         }
      }
      successful = true;
      return computeResult;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public BiFunction getRemappingBiFunction() {
      return remappingBiFunction;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeBoolean(computeIfPresent);
      output.writeObject(remappingBiFunction);
      output.writeObject(metadata);
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      computeIfPresent = input.readBoolean();
      remappingBiFunction = (BiFunction) input.readObject();
      metadata = (Metadata) input.readObject();
      commandInvocationId = CommandInvocationId.readFrom(input);
      setFlagsBitSet(input.readLong());
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitComputeCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.PRIMARY;
   }

   @Override
   public void initBackupWriteRpcCommand(BackupWriteRpcCommand command) {
      command.setCompute(commandInvocationId, key, remappingBiFunction, computeIfPresent, metadata, getFlagsBitSet(), getTopologyId());
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      ComputeCommand that = (ComputeCommand) o;

      if (!Objects.equals(metadata, that.metadata)) return false;
      if (!Objects.equals(computeIfPresent, that.computeIfPresent)) return false;
      return Objects.equals(remappingBiFunction, this.remappingBiFunction);
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), computeIfPresent, remappingBiFunction, metadata);
   }

   @Override
   public String toString() {
      return "ComputeCommand{" +
            "key=" + toStr(key) +
            ", isComputeIfPresent=" + toStr(computeIfPresent) +
            ", remappingBiFunction=" + toStr(remappingBiFunction) +
            ", metadata=" + metadata +
            ", flags=" + printFlags() +
            ", successful=" + isSuccessful() +
            ", valueMatcher=" + getValueMatcher() +
            ", topologyId=" + getTopologyId() +
            '}';
   }

   @Override
   public final boolean isReturnValueExpected() {
      return true;
   }
}
