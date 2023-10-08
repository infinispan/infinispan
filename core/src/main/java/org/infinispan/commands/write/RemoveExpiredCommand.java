package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.TimeoutException;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;


/**
 * Removes an entry that is expired from memory
 *
 * @author William Burns
 * @since 8.0
 */
@ProtoTypeId(ProtoStreamTypeIds.REMOVE_EXPIRED_COMMAND)
public class RemoveExpiredCommand extends RemoveCommand {
   public static final int COMMAND_ID = 58;

   @ProtoField(11)
   boolean maxIdle;

   @ProtoField(12)
   Long lifespan;

   @ProtoFactory
   RemoveExpiredCommand(MarshallableObject<?> wrappedKey, int segment, int topologyId, long flagsWithoutRemote,
                        CommandInvocationId commandInvocationId, MarshallableObject<?> wrappedValue,
                        MarshallableObject<Metadata> wrappedMetadata, ValueMatcher valueMatcher,
                        PrivateMetadata internalMetadata, boolean returnEntryNecessary, boolean maxIdle, Long lifespan) {
      super(wrappedKey, flagsWithoutRemote, topologyId, segment, commandInvocationId, wrappedValue, null,
            valueMatcher, internalMetadata, returnEntryNecessary);
      this.maxIdle = maxIdle;
      this.lifespan = lifespan;
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
   public boolean shouldReplicate(InvocationContext ctx, boolean requireReplicateIfRemote) {
      // TODO: I think expiration always has to replicate - check if works later
      return isSuccessful();
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
