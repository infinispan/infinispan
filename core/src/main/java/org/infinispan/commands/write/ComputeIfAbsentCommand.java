package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Objects;
import java.util.function.Function;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.COMPUTE_IF_ABSENT_COMMAND)
public class ComputeIfAbsentCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {

   private Function<?, ?> mappingFunction;
   private Metadata metadata;
   private PrivateMetadata internalMetadata;
   private transient boolean successful = true;

   public ComputeIfAbsentCommand(Object key, Function<?, ?> mappingFunction, int segment, long flagsBitSet,
                                 CommandInvocationId commandInvocationId, Metadata metadata) {
      super(key, segment, flagsBitSet, commandInvocationId);
      this.mappingFunction = mappingFunction;
      this.setMetadata(metadata);
   }

   @ProtoFactory
   ComputeIfAbsentCommand(MarshallableObject<?> wrappedKey, long flagsWithoutRemote, int topologyId, int segment,
                          CommandInvocationId commandInvocationId, MarshallableObject<Function<?, ?>> wrappedMappingFunction,
                          MarshallableObject<Metadata> wrappedMetadata, PrivateMetadata internalMetadata) {
      super(wrappedKey, flagsWithoutRemote, topologyId, segment, commandInvocationId);
      this.mappingFunction = MarshallableObject.unwrap(wrappedMappingFunction);
      this.metadata = MarshallableObject.unwrap(wrappedMetadata);
      this.internalMetadata = internalMetadata;
   }

   @ProtoField(number = 6, name = "mappingFunction")
   MarshallableObject<Function<?, ?>> getWrappedMappingFunction() {
      return MarshallableObject.create(mappingFunction);
   }

   @ProtoField(number = 7, name = "metadata")
   MarshallableObject<Metadata> getWrappedMetadata() {
      return MarshallableObject.create(metadata);
   }

   @Override
   @ProtoField(8)
   public PrivateMetadata getInternalMetadata() {
      return internalMetadata;
   }

   @Override
   public void setInternalMetadata(PrivateMetadata internalMetadata) {
      this.internalMetadata = internalMetadata;
   }

   @Override
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

   @Override
   public final boolean isReturnValueExpected() {
      return true;
   }

   public Function getMappingFunction() {
      return mappingFunction;
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
      return Objects.equals(mappingFunction, that.mappingFunction);
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
}
