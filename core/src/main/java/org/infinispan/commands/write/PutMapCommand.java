package org.infinispan.commands.write;

import org.infinispan.commands.AbstractFlagAffectedCommand;
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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PutMapCommand extends AbstractFlagAffectedCommand implements WriteCommand, MetadataAwareCommand, RemoteLockCommand {
   public static final byte COMMAND_ID = 9;

   Map<Object, Object> map;
   CacheNotifier notifier;
   Metadata metadata;
   boolean isForwarded = false;
   private CommandInvocationId commandInvocationId;

   public PutMapCommand() {
   }

   public PutMapCommand(Map<?, ?> map, CacheNotifier notifier, Metadata metadata, Set<Flag> flags, CommandInvocationId commandInvocationId) {
      this.map = (Map<Object, Object>) map;
      this.notifier = notifier;
      this.metadata = metadata;
      this.flags = flags;
      this.commandInvocationId = commandInvocationId;
   }

   public PutMapCommand(PutMapCommand command) {
      this.map = command.map;
      this.notifier = command.notifier;
      this.metadata = command.metadata;
      this.flags = command.flags;
      this.isForwarded = command.isForwarded;
      this.commandInvocationId = CommandInvocationId.generateIdFrom(command.commandInvocationId);
   }

   public void init(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPutMapCommand(ctx, this);
   }

   @Override
   public Collection<Object> getKeysToLock() {
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

   private MVCCEntry lookupMvccEntry(InvocationContext ctx, Object key) {
      return (MVCCEntry) ctx.lookupEntry(key);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // The previous values map is only used by the query interceptor to locate the index for the old value
      Map<Object, Object> previousValues = new HashMap<>();
      for (Entry<Object, Object> e : map.entrySet()) {
         Object key = e.getKey();
         MVCCEntry contextEntry = lookupMvccEntry(ctx, key);
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
      output.writeObject(Flag.copyWithoutRemotableFlags(flags));
      output.writeObject(commandInvocationId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      map = (Map<Object, Object>) input.readObject();
      metadata = (Metadata) input.readObject();
      isForwarded = input.readBoolean();
      flags = (Set<Flag>) input.readObject();
      commandInvocationId = (CommandInvocationId) input.readObject();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PutMapCommand that = (PutMapCommand) o;

      if (metadata != null ? !metadata.equals(that.metadata) : that.metadata != null) return false;
      if (map != null ? !map.equals(that.map) : that.map != null) return false;

      return true;
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
            sb.append(e.getKey()).append('=').append(e.getValue());
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
      sb.append("}, flags=").append(flags)
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
   public Set<Object> getAffectedKeys() {
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
