package org.infinispan.commands.functional;

import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.functional.impl.EntryViews;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.infinispan.functional.impl.EntryViews.snapshot;

public final class ReadOnlyManyCommand<K, V, R> extends AbstractDataCommand implements LocalCommand {

   private Set<? extends K> keys;
   private Function<ReadEntryView<K, V>, R> f;

   private ConsistentHash ch;
   // TODO: remotely fetched are because of compatibility - can't we just always return InternalCacheEntry and have
   //       the unboxing executed as the topmost interceptor?
   private Map<Object, InternalCacheEntry> remotelyFetched;

   public ReadOnlyManyCommand(Set<? extends K> keys, Function<ReadEntryView<K, V>, R> f) {
      this.keys = keys;
      this.f = f;
   }

   public ReadOnlyManyCommand() {
   }

   public Set<? extends K> getKeys() {
      return keys;
   }

   @Override
   public byte getCommandId() {
      return -1;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      // Not really replicated
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      // Not really replicated
   }

   public ConsistentHash getConsistentHash() {
      return ch;
   }

   public void setConsistentHash(ConsistentHash ch) {
      this.ch = ch;
   }

   public Map<Object, InternalCacheEntry> getRemotelyFetched() {
      return remotelyFetched;
   }

   public void setRemotelyFetched(Map<Object, InternalCacheEntry> remotelyFetched) {
      this.remotelyFetched = remotelyFetched;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      return keys.stream().map(k -> {
         CacheEntry<K, V> me = lookupCacheEntry(ctx, k);
         R ret = f.apply(me == null ? EntryViews.noValue(k) : EntryViews.readOnly(me));
         return snapshot(ret);
      });
   }

   private CacheEntry<K, V> lookupCacheEntry(InvocationContext ctx, Object key) {
      return ctx.lookupEntry(key);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadOnlyManyCommand(ctx, this);
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
   public String toString() {
      return "ReadOnlyManyCommand{" +
         "keys=" + keys +
         ", f=" + f +
         ", ch=" + ch +
         ", remotelyFetched=" + remotelyFetched +
         '}';
   }
}
