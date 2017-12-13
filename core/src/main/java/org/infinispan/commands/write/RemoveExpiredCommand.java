package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commons.util.EnumUtil;
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

   private Long lifespan;
   private IncrementableEntryVersion nonExistentVersion;

   public RemoveExpiredCommand() {
      // The value matcher will always be the same, so we don't need to serialize it like we do for the other commands
      this.valueMatcher = ValueMatcher.MATCH_EXPECTED_OR_NULL;
   }

   public RemoveExpiredCommand(Object key, Object value, Long lifespan, CacheNotifier notifier,
                               CommandInvocationId commandInvocationId, IncrementableEntryVersion nonExistentVersion) {
      //valueEquivalence can be null because this command never compares values.
      super(key, value, notifier, EnumUtil.EMPTY_BIT_SET, commandInvocationId);
      this.lifespan = lifespan;
      this.valueMatcher = ValueMatcher.MATCH_EXPECTED_OR_NULL;
      this.nonExistentVersion = nonExistentVersion;
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
         Object value = e.getValue();
         // If the provided lifespan is null, that means it is a store removal command, so we can't compare lifespan
         Object prevValue = e.getValue();
         Metadata metadata = e.getMetadata();
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
            // TODO: add a threshold to verify this wasn't just created with the same value/lifespan just before expiring
            if (valueMatcher.matches(prevValue, value, null)) {
               e.setExpired(true);
               return performRemove(e, prevValue, ctx);
            }
         } else {
            log.trace("Cannot remove entry as its lifespan or value do not match");
         }
      } else {
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
   public void notify(InvocationContext ctx, Object removedValue, Metadata removedMetadata,
                      boolean isPre) {
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
              '}';
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeObject(key);
      output.writeObject(value);
      if (lifespan != null) {
         output.writeBoolean(true);
         output.writeLong(lifespan);
      } else {
         output.writeBoolean(false);
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      key = input.readObject();
      value = input.readObject();
      boolean lifespanProvided = input.readBoolean();
      if (lifespanProvided) {
         lifespan = input.readLong();
      } else {
         lifespan = null;
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      RemoveExpiredCommand that = (RemoveExpiredCommand) o;
      return Objects.equals(lifespan, that.lifespan);
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), lifespan);
   }

   @Override
   public long getFlagsBitSet() {
      // Override the flags
      return FlagBitSets.SKIP_CACHE_LOAD;
   }

   @Override
   public void initBackupWriteRpcCommand(BackupWriteRpcCommand command) {
      command.setRemoveExpired(commandInvocationId, key, value, FlagBitSets.SKIP_CACHE_LOAD, getTopologyId());
   }

   public void init(CacheNotifier notifier, IncrementableEntryVersion nonExistentVersion) {
      super.init(notifier);
      this.nonExistentVersion = nonExistentVersion;

   }
}
