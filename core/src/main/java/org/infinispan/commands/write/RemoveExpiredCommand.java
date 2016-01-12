package org.infinispan.commands.write;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;


/**
 * Removes an entry that is expired from memory
 *
 * @author William Burns
 * @since 8.0
 */
public class RemoveExpiredCommand extends RemoveCommand {
   public static final int COMMAND_ID = 58;
   private static final Log log = LogFactory.getLog(RemoveExpiredCommand.class);

   protected Long lifespan;
   protected TimeService timeService;

   public RemoveExpiredCommand() {
      // The value matcher will always be the same, so we don't need to serialize it like we do for the other commands
      this.valueMatcher = ValueMatcher.MATCH_EXPECTED_OR_NULL;
   }

   public RemoveExpiredCommand(Object key, Object value, Long lifespan, CacheNotifier notifier,
           Equivalence valueEquivalence, TimeService timeService, CommandInvocationId commandInvocationId) {
      //valueEquivalence can be null because this command never compares values.
      super(key, value, notifier, EnumUtil.EMPTY_BIT_SET, valueEquivalence, commandInvocationId);
      this.lifespan = lifespan;
      this.timeService = timeService;
      this.valueMatcher = ValueMatcher.MATCH_EXPECTED_OR_NULL;
   }

   public void init(CacheNotifier notifier, Configuration configuration, TimeService timeService) {
      super.init(notifier, configuration);
      this.timeService = timeService;
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
         if (lifespan == null) {
            if (valueMatcher.matches(e, value, e.getValue(), valueEquivalence)) {
               e.setExpired(true);
               return performRemove(e, ctx);
            }
         } else if (e.getMetadata() == null) {
            // If there is no metadata and no value that means it is gone currently or not shown due to expired
            // If we have a value though we should verify it matches the value as well
            if (value == null || valueMatcher.matches(e, value, e.getValue(), valueEquivalence)) {
               e.setExpired(true);
               return performRemove(e, ctx);
            }
         } else if (e.getLifespan() > 0 && e.getLifespan() == lifespan) {
            // If the entries lifespan is not positive that means it can't expire so don't even try to remove it
            // Lastly if there is metadata we have to verify it equals our lifespan and the value match.
            // TODO: add a threshold to verify this wasn't just created with the same value/lifespan just before expiring
            if (valueMatcher.matches(e, value, e.getValue(), valueEquivalence)) {
               e.setExpired(true);
               return performRemove(e, ctx);
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
              "key=" + key +
              "value=" + value +
              "lifespan=" + lifespan +
              '}';
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(commandInvocationId);
      output.writeObject(key);
      output.writeObject(value);
      output.writeLong(lifespan);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = (CommandInvocationId) input.readObject();
      key = input.readObject();
      value = input.readObject();
      lifespan = input.readLong();
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      switch (status) {
         case FAILED:
         case STOPPING:
         case TERMINATED:
            return true;
         default:
            return false;
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
   public Set<Flag> getFlags() {
      return EnumSet.of(Flag.SKIP_CACHE_LOAD);
   }

   @Override
   public boolean hasFlag(Flag flag) {
      // We skip cache load, since if the entry is not in memory then it wasn't updated since it last expired
      return flag == Flag.SKIP_CACHE_LOAD;
   }
}
