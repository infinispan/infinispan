package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.Metadatas;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PutMapCommand extends AbstractTopologyAffectedCommand implements WriteCommand, MetadataAwareCommand, RemoteLockCommand {
   public static final byte COMMAND_ID = 9;

   private Map<Object, Object> map;
   private CacheNotifier<Object, Object> notifier;
   private Metadata metadata;
   private boolean isForwarded = false;

   public CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
   }

   private CommandInvocationId commandInvocationId;

   public PutMapCommand() {
   }

   @SuppressWarnings("unchecked")
   public PutMapCommand(Map<?, ?> map, CacheNotifier notifier, Metadata metadata, long flagsBitSet, CommandInvocationId commandInvocationId) {
      this.map = (Map<Object, Object>) map;
      this.notifier = notifier;
      this.metadata = metadata;
      this.commandInvocationId = commandInvocationId;
      setFlagsBitSet(flagsBitSet);
   }

   public PutMapCommand(PutMapCommand command) {
      this(command, true);
   }

   public PutMapCommand(PutMapCommand command, boolean generateNewId) {
      this.map = command.map;
      this.notifier = command.notifier;
      this.metadata = command.metadata;
      this.isForwarded = command.isForwarded;
      this.commandInvocationId = generateNewId ?
            CommandInvocationId.generateIdFrom(command.commandInvocationId) :
            command.commandInvocationId;
      setFlagsBitSet(command.getFlagsBitSet());
   }

   public void init(CacheNotifier<Object, Object> notifier) {
      this.notifier = notifier;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPutMapCommand(ctx, this);
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
      return hasFlag(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
   }

   @Override
   public boolean hasSkipLocking() {
      return hasFlag(Flag.SKIP_LOCKING);
   }

   private MVCCEntry<Object, Object> lookupMvccEntry(InvocationContext ctx, Object key) {
      //noinspection unchecked
      return (MVCCEntry) ctx.lookupEntry(key);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // The previous values map is only used by the query interceptor to locate the index for the old value
      Map<Object, Object> previousValues = new HashMap<>();
      for (Entry<Object, Object> e : map.entrySet()) {
         Object key = e.getKey();
         MVCCEntry<Object, Object> contextEntry = lookupMvccEntry(ctx, key);
         if (contextEntry != null) {
            Object newValue = e.getValue();
            Object previousValue = contextEntry.getValue();
            Metadata previousMetadata = contextEntry.getMetadata();

            // Even though putAll() returns void, QueryInterceptor reads the previous values
            // TODO The previous values are not correct if the entries exist only in a store
            previousValues.put(key, previousValue);

            if (contextEntry.isCreated()) {
               notifier.notifyCacheEntryCreated(key, newValue, metadata, true, ctx, this);
            } else {
               notifier.notifyCacheEntryModified(key, newValue, metadata, previousValue, previousMetadata,
                                                 true, ctx, this);
            }
            contextEntry.setValue(newValue);
            Metadatas.updateMetadata(contextEntry, metadata);
            contextEntry.setChanged(true);
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

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(map);
      output.writeObject(metadata);
      output.writeBoolean(isForwarded);
      output.writeLong(Flag.copyWithoutRemotableFlags(getFlagsBitSet()));
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
         .append("}");
      return sb.toString();
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
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
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      // Do nothing
   }

   @Override
   public Collection<?> getAffectedKeys() {
      return map.keySet();
   }

   @Override
   public void updateStatusFromRemoteResponse(Object remoteResponse) {
      // Do nothing
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   @Override
   public boolean readsExistingValues() {
      return false;
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
}
