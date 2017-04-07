package org.infinispan.commands.functional;

import static org.infinispan.functional.impl.EntryViews.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;

// TODO: the command does not carry previous values to backup, so it can cause
// the values on primary and backup owners to diverge in case of topology change
public final class ReadWriteManyEntriesCommand<K, V, R> extends AbstractWriteManyCommand<K, V> implements StrictOrderingCommand<K> {

   public static final byte COMMAND_ID = 53;

   private Map<? extends K, ? extends V> entries;
   private BiFunction<V, ReadWriteEntryView<K, V>, R> f;
   private Map<K, CommandInvocationId> lastInvocationIds;

   private int topologyId = -1;
   boolean isForwarded = false;

   public ReadWriteManyEntriesCommand(Map<? extends K, ? extends V> entries, BiFunction<V, ReadWriteEntryView<K, V>, R> f, Params params, CommandInvocationId commandInvocationId) {
      super(commandInvocationId);
      this.entries = entries;
      this.f = f;
      this.params = params;
   }

   public ReadWriteManyEntriesCommand() {
   }

   public ReadWriteManyEntriesCommand(ReadWriteManyEntriesCommand command) {
      this.commandInvocationId = command.commandInvocationId;
      this.entries = command.entries;
      this.f = command.f;
      this.params = command.params;
      this.flags = command.flags;
      this.topologyId = command.topologyId;
      this.lastInvocationIds = command.lastInvocationIds;
   }

   public Map<? extends K, ? extends V> getEntries() {
      return entries;
   }

   public void setEntries(Map<? extends K, ? extends V> entries) {
      this.entries = entries;
   }

   public final ReadWriteManyEntriesCommand<K, V, R> withEntries(Map<? extends K, ? extends V> entries) {
      setEntries(entries);
      return this;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeObject(entries);
      output.writeObject(f);
      output.writeBoolean(isForwarded);
      Params.writeObject(output, params);
      output.writeLong(flags);
      MarshallUtil.marshallMap(lastInvocationIds, ObjectOutput::writeObject, CommandInvocationId::writeTo, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      entries = (Map<? extends K, ? extends V>) input.readObject();
      f = (BiFunction<V, ReadWriteEntryView<K, V>, R>) input.readObject();
      isForwarded = input.readBoolean();
      params = Params.readObject(input);
      flags = input.readLong();
      lastInvocationIds = MarshallUtil.unmarshallMap(input, in -> (K) in.readObject(), CommandInvocationId::readFrom, HashMap::new);
   }

   public boolean isForwarded() {
      return isForwarded;
   }

   public void setForwarded(boolean forwarded) {
      isForwarded = forwarded;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public int getTopologyId() {
      return topologyId;  // TODO: Customise this generated block
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadWriteManyEntriesCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      List<R> returns = new ArrayList<>(entries.size());
      entries.forEach((k, v) -> {
         CacheEntry<K, V> entry = ctx.lookupEntry(k);

         if (entry == null) {
            throw new IllegalStateException();
         }
         R r = snapshot(f.apply(v, EntryViews.readWrite(entry)));
         recordInvocation(entry, r);
         returns.add(r);
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
   public Collection<?> getAffectedKeys() {
      return entries.keySet();
   }

   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ReadWriteManyEntriesCommand{");
      sb.append("entries=").append(entries);
      sb.append(", f=").append(f.getClass().getName());
      sb.append(", isForwarded=").append(isForwarded);
      sb.append(", topologyId=").append(topologyId);
      sb.append(", commandInvocationId=").append(commandInvocationId);
      sb.append('}');
      return sb.toString();
   }

   @Override
   public Collection<Object> getKeysToLock() {
      // TODO: fixup the generics
      return (Collection<Object>) entries.keySet();
   }

   @Override
   public Mutation<K, V, ?> toMutation(K key) {
      return new Mutations.ReadWriteWithValue(entries.get(key), f);
   }

   @Override
   public CommandInvocationId getLastInvocationId(K key) {
      if (lastInvocationIds == null) {
         // if the command was executed on clear cache, last invocation id is not set at all
         return null;
      }
      return lastInvocationIds.get(key);
   }

   @Override
   public void setLastInvocationId(K key, CommandInvocationId id) {
      if (lastInvocationIds == null) {
         lastInvocationIds = new HashMap<>();
      }
      lastInvocationIds.put(key, id);
   }
}
