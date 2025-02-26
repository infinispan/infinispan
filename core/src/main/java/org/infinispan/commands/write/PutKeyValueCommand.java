package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Implements functionality defined by {@link org.infinispan.Cache#put(Object, Object)}
 *
 * <p>Note: Since 9.4, when the flag {@link org.infinispan.context.Flag#PUT_FOR_STATE_TRANSFER} is set,
 * the metadata is actually an {@code InternalMetadata} that includes the timestamps of the entry
 * from the source node.</p>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.PUT_KEY_VALUE_COMMAND)
public class PutKeyValueCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {

   private Object value;
   private boolean putIfAbsent;
   private boolean returnEntry;
   private Metadata metadata;
   private ValueMatcher valueMatcher;
   private PrivateMetadata internalMetadata;

   private transient boolean successful = true;

   public PutKeyValueCommand(Object key, Object value, boolean putIfAbsent, boolean returnEntry, Metadata metadata,
                             int segment, long flagsBitSet, CommandInvocationId commandInvocationId) {
      super(key, segment, flagsBitSet, commandInvocationId);
      this.value = value;
      this.putIfAbsent = putIfAbsent;
      this.returnEntry = returnEntry;
      this.valueMatcher = putIfAbsent ? ValueMatcher.MATCH_EXPECTED : ValueMatcher.MATCH_ALWAYS;
      this.metadata = metadata;
   }

   @ProtoFactory
   PutKeyValueCommand(MarshallableObject<?> wrappedKey, long flagsWithoutRemote, int topologyId, int segment,
                      CommandInvocationId commandInvocationId, MarshallableObject<?> wrappedValue,
                      MarshallableObject<Metadata> wrappedMetadata, ValueMatcher valueMatcher, boolean putIfAbsent,
                      PrivateMetadata internalMetadata, boolean returnEntryNecessary) {
      super(wrappedKey, flagsWithoutRemote, topologyId, segment, commandInvocationId);
      this.value = MarshallableObject.unwrap(wrappedValue);
      this.metadata = MarshallableObject.unwrap(wrappedMetadata);
      this.valueMatcher = valueMatcher;
      this.putIfAbsent = putIfAbsent;
      this.internalMetadata = internalMetadata;
      this.returnEntry = returnEntryNecessary;
   }

   @ProtoField(number = 6, name = "value")
   MarshallableObject<?> getWrappedValue() {
      return MarshallableObject.create(value);
   }

   @ProtoField(number = 7, name = "metadata")
   MarshallableObject<Metadata> getWrappedMetadata() {
      return MarshallableObject.create(metadata);
   }

   @Override
   @ProtoField(8)
   public ValueMatcher getValueMatcher() {
      return valueMatcher;
   }

   @ProtoField(9)
   public boolean isPutIfAbsent() {
      return putIfAbsent;
   }

   @ProtoField(10)
   public PrivateMetadata getInternalMetadata() {
      return internalMetadata;
   }

   @ProtoField(number = 11, name = "returnEntry")
   public boolean isReturnEntryNecessary() {
      return returnEntry;
   }

   public void setInternalMetadata(PrivateMetadata internalMetadata) {
      this.internalMetadata = internalMetadata;
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
      if (isConditional() || !hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES)) {
         return LoadType.PRIMARY;
      } else {
         return LoadType.DONT_LOAD;
      }
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
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

      return putIfAbsent == that.putIfAbsent &&
            Objects.equals(value, that.value) &&
            Objects.equals(metadata, that.metadata);

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
      return new StringBuilder()
            .append("PutKeyValueCommand{key=")
            .append(toStr(key))
            .append(", value=").append(toStr(value))
            .append(", flags=").append(printFlags())
            .append(", commandInvocationId=").append(CommandInvocationId.show(commandInvocationId))
            .append(", putIfAbsent=").append(putIfAbsent)
            .append(", returnEntry=").append(returnEntry)
            .append(", valueMatcher=").append(valueMatcher)
            .append(", metadata=").append(metadata)
            .append(", internalMetadata=").append(internalMetadata)
            .append(", successful=").append(successful)
            .append(", topologyId=").append(getTopologyId())
            .append(", segment=").append(segment)
            .append("}")
            .toString();
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
   public void setValueMatcher(ValueMatcher valueMatcher) {
      this.valueMatcher = valueMatcher;
   }

   @Override
   public void fail() {
      successful = false;
   }

   @Override
   public boolean isReturnValueExpected() {
      return isConditional() || super.isReturnValueExpected();
   }
}
