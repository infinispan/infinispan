package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

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
   private boolean successful = true;
   private boolean returnEntry = false;
   private PrivateMetadata internalMetadata;

   private ValueMatcher valueMatcher;

   public ReplaceCommand() {
   }

   public ReplaceCommand(Object key, Object oldValue, Object newValue, boolean returnEntry,
                         Metadata metadata, int segment, long flagsBitSet,
                         CommandInvocationId commandInvocationId) {
      super(key, segment, flagsBitSet, commandInvocationId);
      this.oldValue = oldValue;
      this.newValue = newValue;
      this.returnEntry = returnEntry;
      this.metadata = metadata;
      this.valueMatcher = oldValue != null ? ValueMatcher.MATCH_EXPECTED : ValueMatcher.MATCH_NON_NULL;
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
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(oldValue);
      output.writeObject(newValue);
      output.writeBoolean(returnEntry);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      output.writeObject(metadata);
      MarshallUtil.marshallEnum(valueMatcher, output);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeObject(internalMetadata);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      oldValue = input.readObject();
      newValue = input.readObject();
      returnEntry = input.readBoolean();
      segment = UnsignedNumeric.readUnsignedInt(input);
      metadata = (Metadata) input.readObject();
      valueMatcher = MarshallUtil.unmarshallEnum(input, ValueMatcher::valueOf);
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
      internalMetadata = (PrivateMetadata) input.readObject();
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
   public ValueMatcher getValueMatcher() {
      return valueMatcher;
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

   public final boolean isReturnEntry() {
      return returnEntry;
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
            ", topologyId=" + getTopologyId() +
            '}';
   }

   @Override
   public PrivateMetadata getInternalMetadata() {
      return internalMetadata;
   }

   @Override
   public void setInternalMetadata(PrivateMetadata internalMetadata) {
      this.internalMetadata = internalMetadata;
   }
}
