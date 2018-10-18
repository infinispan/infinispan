package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.function.Function;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.Metadata;

public class ComputeIfAbsentCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {

   public static final int COMMAND_ID = 69;

   private Function mappingFunction;
   private Metadata metadata;
   private boolean successful = true;

   public ComputeIfAbsentCommand() {
   }

   public ComputeIfAbsentCommand(Object key,
                                 Function mappingFunction,
                                 int segment, long flagsBitSet,
                                 CommandInvocationId commandInvocationId,
                                 Metadata metadata,
                                 ComponentRegistry componentRegistry) {

      super(key, segment, flagsBitSet, commandInvocationId);
      this.mappingFunction = mappingFunction;
      this.metadata = metadata;
      componentRegistry.wireDependencies(this.mappingFunction);
   }

   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(mappingFunction);
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
      return false;
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

   public Function getMappingFunction() {
      return mappingFunction;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(mappingFunction);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      output.writeObject(metadata);
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      mappingFunction = (Function) input.readObject();
      segment = UnsignedNumeric.readUnsignedInt(input);
      metadata = (Metadata) input.readObject();
      commandInvocationId = CommandInvocationId.readFrom(input);
      setFlagsBitSet(input.readLong());
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitComputeIfAbsentCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.PRIMARY;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      ComputeIfAbsentCommand that = (ComputeIfAbsentCommand) o;

      if (!Objects.equals(metadata, that.metadata)) return false;
      return Objects.equals(mappingFunction, this.mappingFunction);
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), mappingFunction, metadata);
   }

   @Override
   public String toString() {
      return "ComputeIfAbsentCommand{" +
            "key=" + toStr(key) +
            ", mappingFunction=" + toStr(mappingFunction) +
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
