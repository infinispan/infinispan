package org.infinispan.commands.functional;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.metadata.Metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.infinispan.functional.impl.EntryViews.snapshot;

public final class ReadWriteManyCommand<K, V, R> implements WriteCommand {

   public static final byte COMMAND_ID = 52;

   private Set<? extends K> keys;
   private Function<ReadWriteEntryView<K, V>, R> f;

   private int topologyId = -1;
   boolean isForwarded = false;
   private List<R> remoteReturns = new ArrayList<>();

   public ReadWriteManyCommand(Set<? extends K> keys, Function<ReadWriteEntryView<K, V>, R> f) {
      this.keys = keys;
      this.f = f;
   }

   public ReadWriteManyCommand(ReadWriteManyCommand command) {
      this.keys = command.keys;
      this.f = command.f;
   }

   public ReadWriteManyCommand() {
   }

   public Set<? extends K> getKeys() {
      return keys;
   }

   public void setKeys(Set<? extends K> keys) {
      this.keys = keys;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallCollection(keys, output);
      output.writeObject(f);
      output.writeBoolean(isForwarded);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallCollection(input, size -> new HashSet<>());
      f = (Function<ReadWriteEntryView<K, V>, R>) input.readObject();
      isForwarded = input.readBoolean();
   }

   public boolean isForwarded() {
      return isForwarded;
   }

   public void setForwarded(boolean forwarded) {
      isForwarded = forwarded;
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   public void addAllRemoteReturns(List<R> returns) {
      remoteReturns.addAll(returns);
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      // No-op
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadWriteManyCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // Can't return a lazy stream here because the current code in
      // EntryWrappingInterceptor expects any changes to be done eagerly,
      // otherwise they're not applied. So, apply the function eagerly and
      // return a lazy stream of the void returns.
      List<R> returns = new ArrayList<>(remoteReturns);
      keys.forEach(k -> {
         CacheEntry<K, V> entry = ctx.lookupEntry(k);

         // Could be that the key is not local, 'null' is how this is signalled
         if (entry != null) {
            R r = f.apply(EntryViews.readWrite(entry));
            returns.add(snapshot(r));
         }
      });
      return returns;
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
   public boolean canBlock() {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public Set<Object> getAffectedKeys() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void updateStatusFromRemoteResponse(Object remoteResponse) {
      // TODO: Customise this generated block
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public boolean readsExistingValues() {
      return true;
   }

   @Override
   public boolean alwaysReadsExistingValues() {
      return false;
   }

   @Override
   public Set<Flag> getFlags() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public long getFlagsBitSet() {
      return EnumUtil.EMPTY_BIT_SET;
   }

   @Override
   public void setFlags(Set<Flag> flags) {
      // TODO: Customise this generated block
   }

   @Override
   public void setFlagsBitSet(long bitSet) {
      // TODO: Customise this generated block
   }

   @Override
   public void setFlags(Flag... flags) {
      // TODO: Customise this generated block
   }

   @Override
   public boolean hasFlag(Flag flag) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public Metadata getMetadata() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void setMetadata(Metadata metadata) {
      // TODO: Customise this generated block
   }

}
