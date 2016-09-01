package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.infinispan.atomic.CopyableDeltaAware;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;

/**
 * Command for a replicable, non-tx, cache.put(k, v) call that uses
 * default metadata and has no listeners attached.
 *
 * - A replicable, non-tx put does not need to be wrapped in another command, it can be sent directly.
 * - Single metadata is default, no metadata information needs to be serialized.
 * - A non-conditional put always matches value, so is always successful.
 * - If value equivalence is default, no need to serialize that info.
 * - Only ignore return values flag set, so no need to serialize that info.
 *
 * @since 9.0
 */
public final class SinglePutKeyValueCommand
   implements
      DataWriteCommand, RemoteLockCommand, FlagAffectedCommand, CacheRpcCommand,
      AdvancedExternalizer<SinglePutKeyValueCommand> {

   public static final byte COMMAND_ID = 61;
   private static final int EXTERNALIZER_ID = COMMAND_ID << 4;

   // Marshallables
   ByteString cacheName;
   Object key;
   CommandInvocationId commandInvocationId;
   Object value;

   public SinglePutKeyValueCommand() {
      // For externalizers
   }

   public SinglePutKeyValueCommand(ByteString cacheName, Object key, CommandInvocationId commandInvocationId, Object value) {
      this.cacheName = cacheName;
      this.key = key;
      this.commandInvocationId = commandInvocationId;
      this.value = value;
   }

   public Object getValue() {
      return value;
   }

   public void setValue(Object value) {
      this.value = value;
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public void setOrigin(Address origin) {
      // No-op, now passed around via PerCacheInboundInvocationHandler.handle() call,
      // then as parameter to BaseBlockingRunnable constructor
   }

   @Override
   public Address getOrigin() {
      return null; // Not in use
   }

   @Override
   public void writeObject(ObjectOutput out, SinglePutKeyValueCommand obj) throws IOException {
      ByteString.writeObject(out, cacheName);
      out.writeObject(key);
      CommandInvocationId.writeTo(out, commandInvocationId);
      out.writeObject(value);
   }

   @Override
   public SinglePutKeyValueCommand readObject(ObjectInput in) throws IOException, ClassNotFoundException {
      ByteString cacheName = ByteString.readObject(in);
      Object key = in.readObject();
      CommandInvocationId commandInvocationId = CommandInvocationId.readFrom(in);
      Object value = in.readObject();
      return new SinglePutKeyValueCommand(cacheName, key, commandInvocationId, value);
   }

   @Override
   public Set<Class<? extends SinglePutKeyValueCommand>> getTypeClasses() {
      return null; // Unused
   }

   @Override
   public Integer getId() {
      return EXTERNALIZER_ID;
   }

   @Override
   public boolean isSuccessful() {
      return true; // Always successful
   }

   @Override
   public boolean isConditional() {
      return false; // Never conditional
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS; // Always matches
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      // No-op
   }

   @Override
   public Set<Object> getAffectedKeys() {
      return Collections.singleton(key);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitSinglePutKeyValueCommand(ctx, this);
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true; // Should be invoked
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   @Override
   public boolean isWriteOnly() {
      return true;
   }

   @Override
   public void updateStatusFromRemoteResponse(Object remoteResponse) {
      // TODO: Customise this generated block
   }

   @Override
   public boolean readsExistingValues() {
      return false;  // Does not need existing value
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);

      //possible as in certain situations (e.g. when locking delegation is used) we don't wrap
      if (e == null) return null;

      return performPut(e, ctx);
   }

   private Object performPut(MVCCEntry e, InvocationContext ctx) {
      Object entryValue = e.getValue();
      Object o;

      if (value instanceof Delta) {
         // magic
         Delta dv = (Delta) value;
         if (e.isRemoved()) {
            e.setExpired(false);
            e.setRemoved(false);
            e.setCreated(true);
            e.setValid(true);
            e.setValue(dv.merge(null));
         } else {
            DeltaAware toMergeWith = null;
            if (entryValue instanceof CopyableDeltaAware) {
               toMergeWith = ((CopyableDeltaAware) entryValue).copy();
            } else if (entryValue instanceof DeltaAware) {
               toMergeWith = (DeltaAware) entryValue;
            }
            e.setValue(dv.merge(toMergeWith));
         }
         o = entryValue;
      } else {
         o = e.setValue(value);
         if (e.isRemoved()) {
            e.setCreated(true);
            e.setExpired(false);
            e.setRemoved(false);
            e.setValid(true);
            o = null;
         }
      }
      e.setChanged(true);
      // Return the expected value when retrying a putIfAbsent command (i.e. null)
      return o;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false; // No return value sent back
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      // No-op since it implements AdvancedExternalizer
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      // No-op since it implements AdvancedExternalizer
   }

   @Override
   public Object getKey() {
      return key;
   }

   @Override
   public long getFlagsBitSet() {
      return EnumUtil.IGNORE_RETURN_VALUES_BIT_SET;
   }

   @Override
   public void setFlagsBitSet(long bitSet) {
      // No-op
   }

   @Override
   public Metadata getMetadata() {
      return null; // No metadata
   }

   @Override
   public void setMetadata(Metadata metadata) {
      // No-op
   }

   @Override
   public Collection<Object> getKeysToLock() {
      return Collections.singleton(key);
   }

   @Override
   public Object getKeyLockOwner() {
      return commandInvocationId;
   }

   @Override
   public boolean hasZeroLockAcquisition() {
      return false;
   }

   @Override
   public boolean hasSkipLocking() {
      return false;
   }

   @Override
   public int getTopologyId() {
      return -1;
   }

   @Override
   public void setTopologyId(int topologyId) {
      // No-op
   }

   public void setKey(Object key) {
      this.key = key;
   }

}
