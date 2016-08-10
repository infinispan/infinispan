package org.infinispan.commands.functional;

import static org.infinispan.functional.impl.EntryViews.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.infinispan.commands.Visitor;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.Params;
import org.infinispan.lifecycle.ComponentStatus;

// TODO: the command does not carry previous values to backup, so it can cause
// the values on primary and backup owners to diverge in case of topology change
public final class ReadWriteManyCommand<K, V, R> extends AbstractWriteManyCommand {

   public static final byte COMMAND_ID = 52;

   private Collection<? extends K> keys;
   private Function<ReadWriteEntryView<K, V>, R> f;

   private int topologyId = -1;
   boolean isForwarded = false;

   public ReadWriteManyCommand(Collection<? extends K> keys, Function<ReadWriteEntryView<K, V>, R> f, Params params) {
      this.keys = keys;
      this.f = f;
      this.params = params;
   }

   public ReadWriteManyCommand(ReadWriteManyCommand command) {
      this.keys = command.keys;
      this.f = command.f;
      this.params = command.params;
   }

   public ReadWriteManyCommand() {
   }

   public void setKeys(Collection<? extends K> keys) {
      this.keys = keys;
   }

   public final ReadWriteManyCommand<K, V, R> withKeys(Collection<? extends K> keys) {
      setKeys(keys);
      return this;
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
      Params.writeObject(output, params);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallCollection(input, ArrayList::new);
      f = (Function<ReadWriteEntryView<K, V>, R>) input.readObject();
      isForwarded = input.readBoolean();
      params = Params.readObject(input);
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

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
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
      List<R> returns = new ArrayList<>(keys.size());
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
   public Collection<?> getAffectedKeys() {
      return keys;
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
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ReadWriteManyCommand{");
      sb.append("keys=").append(keys);
      sb.append(", f=").append(f);
      sb.append(", isForwarded=").append(isForwarded);
      sb.append('}');
      return sb.toString();
   }
}
