package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PutMapCommand extends AbstractTopologyAffectedCommand implements WriteCommand, MetadataAwareCommand, RemoteLockCommand {
   public static final byte COMMAND_ID = 9;

   private Map<Object, Object> map;
   // The map must support null values
   private final Map<?, ?> providedResults;
   private transient CacheNotifier<Object, Object> notifier;
   private Metadata metadata;
   private boolean isForwarded = false;
   private boolean synchronous;
   private transient InvocationManager invocationManager;
   private transient Set<Object> completedKeys = null;
   private CommandInvocationId commandInvocationId;

   public PutMapCommand() {
      providedResults = null;
   }

   @SuppressWarnings("unchecked")
   public PutMapCommand(Map<?, ?> map, CacheNotifier notifier, Metadata metadata, long flagsBitSet,
                        CommandInvocationId commandInvocationId, Map<?, ?> providedResults, InvocationManager invocationManager, boolean synchronous) {
      this.map = (Map<Object, Object>) map;
      this.notifier = notifier;
      this.metadata = metadata;
      this.commandInvocationId = commandInvocationId;
      this.providedResults = providedResults;
      this.invocationManager = invocationManager;
      this.synchronous = synchronous;
      setFlagsBitSet(flagsBitSet);
   }

   public PutMapCommand(PutMapCommand command) {
      this(command, true);
   }

   public PutMapCommand(PutMapCommand command, boolean generateNewId) {
      this.map = command.map;
      this.providedResults = null;
      this.notifier = command.notifier;
      this.metadata = command.metadata;
      this.isForwarded = command.isForwarded;
      this.commandInvocationId = generateNewId ?
            CommandInvocationId.generateIdFrom(command.commandInvocationId) :
            command.commandInvocationId;
      this.setTopologyId(getTopologyId());
      this.invocationManager = command.invocationManager;
      this.synchronous = command.synchronous;
      setFlagsBitSet(command.getFlagsBitSet());
   }

   public void init(CacheNotifier<Object, Object> notifier, InvocationManager invocationManager, boolean synchronous) {
      this.notifier = notifier;
      this.invocationManager = invocationManager;
      this.synchronous = synchronous;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPutMapCommand(ctx, this);
   }

   public CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
   }

   @Override
   public void setAuthoritative(boolean authoritative) {
      // noop, we can already use isForwarded
   }


   @Override
   public Collection<?> getKeysToLock() {
      return isForwarded ? Collections.emptyList() : Collections.unmodifiableCollection(map.keySet());
   }

   @Override
   public Object getKeyLockOwner() {
      return commandInvocationId;
   }

   @Override
   public boolean hasZeroLockAcquisition() {
      return hasAnyFlag(FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT);
   }

   @Override
   public boolean hasSkipLocking() {
      return hasAnyFlag(FlagBitSets.SKIP_LOCKING);
   }

   private MVCCEntry<Object, Object> lookupMvccEntry(InvocationContext ctx, Object key) {
      //noinspection unchecked
      return (MVCCEntry) ctx.lookupEntry(key);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // Previous values are used by the query interceptor to locate the index for the old value
      Map<Object, Object> previousValues = hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES) ? null : new HashMap<>(map.size());
      for (Entry<Object, Object> e : map.entrySet()) {
         Object key = e.getKey();
         MVCCEntry<Object, Object> contextEntry = lookupMvccEntry(ctx, key);
         assert contextEntry != null;

         Object newValue = e.getValue();
         Object previousValue = contextEntry.getValue();
         Metadata previousMetadata = contextEntry.getMetadata();

         // Even though putAll() returns void, QueryInterceptor reads the previous values
         // TODO The previous values are not correct if the entries exist only in a store
         Object returnValue = null;
         if (previousValues != null) {
            returnValue = previousValue;
            if (providedResults != null) {
               // the value might be null, so we have to check using contains
               if (providedResults.containsKey(key)) {
                  returnValue = providedResults.get(key);
               }
            }
            previousValues.put(key, returnValue);
         }

         if (contextEntry.isCreated()) {
            notifier.notifyCacheEntryCreated(key, newValue, this.metadata, true, ctx, this);
         } else {
            notifier.notifyCacheEntryModified(key, newValue, this.metadata, previousValue,
                  previousMetadata, true, ctx, this);
         }

         contextEntry.setValue(newValue);
         contextEntry.setChanged(true);

         Metadata.Builder builder;
         if (this.metadata != null) {
            builder = this.metadata.builder();
            if (this.metadata.version() == null && previousMetadata != null && previousMetadata.version() != null) {
               builder = builder.version(previousMetadata.version());
            }
            if (previousMetadata != null) {
               builder.invocations(previousMetadata.lastInvocation());
            }
         } else if (previousMetadata != null) {
            builder = previousMetadata.builder();
         } else {
            builder = new EmbeddedMetadata.Builder();
         }
         // TODO: explain when id can be null
         /** See {@link InvocationManager#notifyCompleted(CommandInvocationId, Object, int)} to check why we don't store on origin */
         if (!(ctx.isOriginLocal() && synchronous) && commandInvocationId != null && invocationManager != null) {
            long now = invocationManager.wallClockTime();
            InvocationRecord purged = InvocationRecord.purgeExpired(builder.invocations(), now - invocationManager.invocationTimeout());
            builder = builder.invocations(purged).invocation(commandInvocationId,
                  isReturnValueExpected() ? returnValue : null, !isForwarded, contextEntry.isCreated(),
                  !contextEntry.isCreated() && !contextEntry.isRemoved(),
                  contextEntry.isRemoved(), now);
            contextEntry.setMetadata(builder.build());
         } else if (metadata != null) {
            contextEntry.setMetadata(builder.build());
         }
      }
      return previousValues;
   }

   public Map<Object, Object> getMap() {
      return map;
   }

   public void setMap(Map<Object, Object> map) {
      this.map = map;
   }

   public final PutMapCommand withMap(Map<Object, Object> map) {
      setMap(map);
      return this;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(map);
      output.writeObject(metadata);
      output.writeBoolean(isForwarded);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      CommandInvocationId.writeTo(output, commandInvocationId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      //noinspection unchecked
      map = (Map<Object, Object>) input.readObject();
      metadata = (Metadata) input.readObject();
      isForwarded = input.readBoolean();
      setFlagsBitSet(input.readLong());
      commandInvocationId = CommandInvocationId.readFrom(input);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PutMapCommand that = (PutMapCommand) o;

      if (metadata != null ? !metadata.equals(that.metadata) : that.metadata != null) return false;
      return map != null ? map.equals(that.map) : that.map == null;

   }

   @Override
   public int hashCode() {
      int result = map != null ? map.hashCode() : 0;
      result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("PutMapCommand{map={");
      if (!map.isEmpty()) {
         Iterator<Entry<Object, Object>> it = map.entrySet().iterator();
         int i = 0;
         for (;;) {
            Entry<Object, Object> e = it.next();
            sb.append(toStr(e.getKey())).append('=').append(toStr(e.getValue()));
            if (!it.hasNext()) {
               break;
            }
            if (i > 100) {
               sb.append(" ...");
               break;
            }
            sb.append(", ");
            i++;
         }
      }
      sb.append("}, flags=").append(printFlags())
         .append(", metadata=").append(metadata)
         .append(", isForwarded=").append(isForwarded)
         .append(", commandInvocationId=").append(commandInvocationId)
         .append(", topologyId=").append(getTopologyId())
         .append("}");
      return sb.toString();
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public Collection<?> getAffectedKeys() {
      return map.keySet();
   }

   @Override
   public void fail() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isReturnValueExpected() {
      return !hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES);
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public LoadType loadType() {
      return hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES) ? LoadType.DONT_LOAD : LoadType.PRIMARY;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   /**
    * For non transactional caches that support concurrent writes (default), the commands are forwarded between nodes,
    * e.g.:
    * - commands is executed on node A, but some of the keys should be locked on node B
    * - the command is send to the main owner (B)
    * - B tries to acquire lock on the keys it owns, then forwards the commands to the other owners as well
    * - at this last stage, the command has the "isForwarded" flag set to true.
    */
   public boolean isForwarded() {
      return isForwarded;
   }

   /**
    * @see #isForwarded()
    */
   public void setForwarded(boolean forwarded) {
      isForwarded = forwarded;
   }

   @Override
   public void setCompleted(Object key) {
      if (completedKeys == null) {
         completedKeys = new HashSet<>();
      }
      completedKeys.add(key);
   }

   @Override
   public boolean isCompleted(Object key) {
      return completedKeys != null && completedKeys.contains(key);
   }

}
