package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.REPLACE_COMMAND)
public class ReplaceCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {

   private Object oldValue;
   private Object newValue;
   private Metadata metadata;
   private ValueMatcher valueMatcher;
   private boolean returnEntry;
   private PrivateMetadata internalMetadata;
   private transient boolean successful = true;

   public ReplaceCommand(Object key, Object oldValue, Object newValue,  boolean returnEntry, Metadata metadata, int segment, long flagsBitSet,
                         CommandInvocationId commandInvocationId) {
      super(key, segment, flagsBitSet, commandInvocationId);
      this.oldValue = oldValue;
      this.newValue = newValue;
      this.returnEntry = returnEntry;
      this.metadata = metadata;
      this.valueMatcher = oldValue != null ? ValueMatcher.MATCH_EXPECTED : ValueMatcher.MATCH_NON_NULL;
   }

   @ProtoFactory
   ReplaceCommand(MarshallableObject<?> wrappedKey, long flagsWithoutRemote, int topologyId, int segment,
                  CommandInvocationId commandInvocationId, MarshallableObject<?> wrappedOldValue,
                  MarshallableObject<?> wrappedNewValue, MarshallableObject<Metadata> wrappedMetadata, ValueMatcher valueMatcher,
                  PrivateMetadata internalMetadata, boolean returnEntry) {
      super(wrappedKey, flagsWithoutRemote, topologyId, segment, commandInvocationId);
      this.oldValue = MarshallableObject.unwrap(wrappedOldValue);
      this.newValue = MarshallableObject.unwrap(wrappedNewValue);
      this.metadata = MarshallableObject.unwrap(wrappedMetadata);
      this.valueMatcher = valueMatcher;
      this.internalMetadata = internalMetadata;
      this.returnEntry = returnEntry;
   }

   @ProtoField(number = 6, name = "oldValue")
   MarshallableObject<?> getWrappedOldValue() {
      return MarshallableObject.create(oldValue);
   }

   @ProtoField(number = 7, name = "newValue")
   MarshallableObject<?> getWrappedNewValue() {
      return MarshallableObject.create(newValue);
   }

   @ProtoField(number = 8, name = "metadata")
   MarshallableObject<Metadata> getWrappedMetadata() {
      return MarshallableObject.create(metadata);
   }

   @Override
   @ProtoField(9)
   public ValueMatcher getValueMatcher() {
      return valueMatcher;
   }

   @Override
   @ProtoField(10)
   public PrivateMetadata getInternalMetadata() {
      return internalMetadata;
   }

   @Override
   public void setInternalMetadata(PrivateMetadata internalMetadata) {
      this.internalMetadata = internalMetadata;
   }

   @ProtoField(11)
   public final boolean isReturnEntry() {
      return returnEntry;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReplaceCommand(ctx, this);
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

      ReplaceCommand that = (ReplaceCommand) o;

      return Objects.equals(metadata, that.metadata) &&
            Objects.equals(newValue, that.newValue) &&
            Objects.equals(oldValue, that.oldValue);

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
   public void setValueMatcher(ValueMatcher valueMatcher) {
      this.valueMatcher = valueMatcher;
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
      return "ReplaceCommand{" +
            "key=" + toStr(key) +
            ", oldValue=" + toStr(oldValue) +
            ", newValue=" + toStr(newValue) +
            ", metadata=" + metadata +
            ", flags=" + printFlags() +
            ", commandInvocationId=" + CommandInvocationId.show(commandInvocationId) +
            ", successful=" + successful +
            ", valueMatcher=" + valueMatcher +
            ", topologyId=" + topologyId +
            '}';
   }
}
