package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.metadata.Metadata;

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
public class PutKeyValueCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {

   public static final byte COMMAND_ID = 8;

   private Object value;
   private boolean putIfAbsent;
   private boolean successful = true;
   private Metadata metadata;
   private ValueMatcher valueMatcher;

   public PutKeyValueCommand() {
   }

   public PutKeyValueCommand(Object key, Object value, boolean putIfAbsent, Metadata metadata, int segment,
                             long flagsBitSet, CommandInvocationId commandInvocationId) {
      super(key, segment, flagsBitSet, commandInvocationId);
      this.value = value;
      this.putIfAbsent = putIfAbsent;
      this.valueMatcher = putIfAbsent ? ValueMatcher.MATCH_EXPECTED : ValueMatcher.MATCH_ALWAYS;
      //noinspection unchecked
      this.metadata = metadata;
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
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(value);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      output.writeObject(metadata);
      MarshallUtil.marshallEnum(valueMatcher, output);
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      output.writeBoolean(putIfAbsent);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      value = input.readObject();
      segment = UnsignedNumeric.readUnsignedInt(input);
      metadata = (Metadata) input.readObject();
      valueMatcher = MarshallUtil.unmarshallEnum(input, ValueMatcher::valueOf);
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
      return new StringBuilder()
            .append("PutKeyValueCommand{key=")
            .append(toStr(key))
            .append(", value=").append(toStr(value))
            .append(", flags=").append(printFlags())
            .append(", commandInvocationId=").append(CommandInvocationId.show(commandInvocationId))
            .append(", putIfAbsent=").append(putIfAbsent)
            .append(", valueMatcher=").append(valueMatcher)
            .append(", metadata=").append(metadata)
            .append(", successful=").append(successful)
            .append(", topologyId=").append(getTopologyId())
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
   public boolean isReturnValueExpected() {
      return isConditional() || super.isReturnValueExpected();
   }
}
