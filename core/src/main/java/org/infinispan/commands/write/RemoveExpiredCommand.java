package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.util.concurrent.TimeoutException;


/**
 * Removes an entry that is expired from memory
 *
 * @author William Burns
 * @since 8.0
 */
public class RemoveExpiredCommand extends RemoveCommand {
   public static final int COMMAND_ID = 58;

   private boolean maxIdle;
   private Long lifespan;

   public RemoveExpiredCommand() {
      // The value matcher will always be the same, so we don't need to serialize it like we do for the other commands
      this.valueMatcher = ValueMatcher.MATCH_EXPECTED_OR_NULL;
   }

   public RemoveExpiredCommand(Object key, Object value, Long lifespan, boolean maxIdle, int segment,
                               long flagBitSet, CommandInvocationId commandInvocationId) {
      //valueEquivalence can be null because this command never compares values.
      super(key, value, false, segment, flagBitSet, commandInvocationId);
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.valueMatcher = ValueMatcher.MATCH_EXPECTED_OR_NULL;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitRemoveExpiredCommand(ctx, this);
   }

   @Override
   public boolean isConditional() {
      return true;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "RemoveExpiredCommand{" +
              "key=" + toStr(key) +
              ", value=" + toStr(value) +
              ", lifespan=" + lifespan +
              ", maxIdle=" + maxIdle +
              ", internalMetadata=" + getInternalMetadata() +
              '}';
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeObject(key);
      output.writeObject(value);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      if (lifespan != null) {
         output.writeBoolean(true);
         output.writeLong(lifespan);
      } else {
         output.writeBoolean(false);
      }
      output.writeBoolean(maxIdle);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      output.writeObject(getInternalMetadata());

   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      key = input.readObject();
      value = input.readObject();
      segment = UnsignedNumeric.readUnsignedInt(input);
      boolean lifespanProvided = input.readBoolean();
      if (lifespanProvided) {
         lifespan = input.readLong();
      } else {
         lifespan = null;
      }
      maxIdle = input.readBoolean();
      setFlagsBitSet(input.readLong());
      setInternalMetadata((PrivateMetadata) input.readObject());
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      RemoveExpiredCommand that = (RemoveExpiredCommand) o;
      return maxIdle == that.maxIdle && Objects.equals(lifespan, that.lifespan);
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), lifespan, maxIdle);
   }

   /**
    * Whether this remove expired was fired because of max idle
    * @return if this command is max idle based expiration
    */
   public boolean isMaxIdle() {
      return maxIdle;
   }

   public Long getLifespan() {
      return lifespan;
   }

   @Override
   public boolean logThrowable(Throwable t) {
      Throwable cause = t;
      do {
         if (cause instanceof TimeoutException) {
            return !hasAnyFlag(FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT);
         }
      } while ((cause = cause.getCause()) != null);
      return true;
   }
}
