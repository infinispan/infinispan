package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.time.TimeService;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * Removes an entry that is expired from memory
 *
 * @author William Burns
 * @since 8.0
 */
public class RemoveExpiredCommand extends RemoveCommand {
   public static final int COMMAND_ID = 58;
   private static final Log log = LogFactory.getLog(RemoveExpiredCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   // The amount in milliseconds of a buffer we allow the system clock to be off, but still allow expiration removal
   private static final int CLOCK_BUFFER = 100;

   private boolean maxIdle;
   private Long lifespan;
   private IncrementableEntryVersion nonExistentVersion;
   private TimeService timeService;

   public RemoveExpiredCommand() {
      // The value matcher will always be the same, so we don't need to serialize it like we do for the other commands
      this.valueMatcher = ValueMatcher.MATCH_EXPECTED_OR_NULL;
   }

   public RemoveExpiredCommand(Object key, Object value, Long lifespan, boolean maxIdle, CacheNotifier notifier, int segment,
                               long flagBitSet, CommandInvocationId commandInvocationId, IncrementableEntryVersion nonExistentVersion,
         TimeService timeService) {
      //valueEquivalence can be null because this command never compares values.
      super(key, value, notifier, segment, flagBitSet, commandInvocationId);
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.valueMatcher = ValueMatcher.MATCH_EXPECTED_OR_NULL;
      this.nonExistentVersion = nonExistentVersion;
      this.timeService = timeService;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitRemoveExpiredCommand(ctx, this);
   }

   /**
    * Performs an expiration on a specified entry
    *
    * @param ctx invocation context
    * @return null
    */
   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
      if (e != null && !e.isRemoved()) {
         Object prevValue = e.getValue();
         Metadata metadata = e.getMetadata();
         // If the provided lifespan is null, that means it is a store removal command, so we can't compare lifespan
         if (lifespan == null) {
            if (valueMatcher.matches(prevValue, value, null)) {
               e.setExpired(true);
               return performRemove(e, prevValue, ctx);
            }
         } else if (metadata == null || metadata.version() == nonExistentVersion) {
            // If there is no metadata and no value that means it is gone currently or not shown due to expired
            // Non existent version is used when versioning is enabled and the entry doesn't exist
            // If we have a value though we should verify it matches the value as well
            if (value == null || valueMatcher.matches(prevValue, value, null)) {
               e.setExpired(true);
               return performRemove(e, prevValue, ctx);
            }
         } else if (e.getLifespan() > 0 && e.getLifespan() == lifespan) {
            // If the entries lifespan is not positive that means it can't expire so don't even try to remove it
            // Lastly if there is metadata we have to verify it equals our lifespan and the value match.
            if (valueMatcher.matches(prevValue, value, null)) {
               // Only remove the entry if it is still expired.
               // Due to difference in system clocks between nodes - we also accept the expiration within 100 ms
               // of now. This along with the fact that entries are not normally access immediately when they
               // expire should give a good enough buffer range to not create a false positive.
               if (ExpiryHelper.isExpiredMortal(lifespan, e.getCreated(), timeService.wallClockTime() + CLOCK_BUFFER)) {
                  if (trace) {
                     log.tracef("Removing entry as its lifespan and value match and it created on %s with a current time of %s",
                           e.getCreated(), timeService.wallClockTime());
                  }
                  e.setExpired(true);
                  return performRemove(e, prevValue, ctx);
               } else if (trace) {
                  log.tracef("Cannot remove entry due to it not being expired - this can be caused by different " +
                        "clocks on nodes or a concurrent write");
               }
            }
         } else if (trace) {
            log.trace("Cannot remove entry as its lifespan or value do not match");
         }
      } else if (trace) {
         log.trace("Nothing to remove since the entry doesn't exist in the context or it is already removed");
      }
      successful = false;
      return false;
   }

   @Override
   public boolean isConditional() {
      return true;
   }

   @Override
   public void notify(InvocationContext ctx, Object removedValue, Metadata removedMetadata, boolean isPre) {
      if (!isPre) {
         notifier.notifyCacheEntryExpired(key, value, removedMetadata, ctx);
      }
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
              ", maxIde=" + maxIdle +
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

   public void init(CacheNotifier notifier, IncrementableEntryVersion nonExistentVersion, TimeService timeService) {
      super.init(notifier);
      this.nonExistentVersion = nonExistentVersion;
      this.timeService = timeService;
   }
}
