package org.infinispan.functional.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.commons.api.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.commons.api.functional.Listeners.WriteListeners;
import org.infinispan.commons.api.functional.Param;
import org.infinispan.commons.api.functional.Param.FutureMode;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.infinispan.functional.impl.Params.withFuture;

/**
 * Write-only map implementation.
 *
 * @since 8.0
 */
@Experimental
public final class WriteOnlyMapImpl<K, V> extends AbstractFunctionalMap<K, V> implements WriteOnlyMap<K, V> {
   private static final Log log = LogFactory.getLog(WriteOnlyMapImpl.class);
   private final Params params;

   private WriteOnlyMapImpl(Params params, FunctionalMapImpl<K, V> functionalMap) {
      super(functionalMap);
      this.params = params;
   }

   public static <K, V> WriteOnlyMap<K, V> create(FunctionalMapImpl<K, V> functionalMap) {
      return new WriteOnlyMapImpl<>(Params.from(functionalMap.params.params), functionalMap);
   }

   private static <K, V> WriteOnlyMap<K, V> create(Params params, FunctionalMapImpl<K, V> functionalMap) {
      return new WriteOnlyMapImpl<>(params, functionalMap);
   }

   @Override
   public CompletableFuture<Void> eval(K key, Consumer<WriteEntryView<V>> f) {
      log.tracef("Invoked eval(k=%s, %s)", key, params);
      Param<FutureMode> futureMode = params.get(FutureMode.ID);
      WriteOnlyKeyCommand cmd = fmap.cmdFactory().buildWriteOnlyKeyCommand(key, f, params);
      InvocationContext ctx = fmap.invCtxFactory().createInvocationContext(true, 1);
      ctx.setLockOwner(cmd.getKeyLockOwner());
      return futureVoid(futureMode, ctx, cmd);
   }

   @Override
   public CompletableFuture<Void> eval(K key, V value, BiConsumer<V, WriteEntryView<V>> f) {
      log.tracef("Invoked eval(k=%s, v=%s, %s)", key, value, params);
      Param<FutureMode> futureMode = params.get(FutureMode.ID);
      WriteOnlyKeyValueCommand cmd = fmap.cmdFactory().buildWriteOnlyKeyValueCommand(key, value, f, params);
      InvocationContext ctx = fmap.invCtxFactory().createInvocationContext(true, 1);
      ctx.setLockOwner(cmd.getKeyLockOwner());
      return futureVoid(futureMode, ctx, cmd);
   }

   @Override
   public CompletableFuture<Void> evalMany(Map<? extends K, ? extends V> entries, BiConsumer<V, WriteEntryView<V>> f) {
      log.tracef("Invoked evalMany(entries=%s, %s)", entries, params);
      Param<FutureMode> futureMode = params.get(FutureMode.ID);
      WriteOnlyManyEntriesCommand cmd = fmap.cmdFactory().buildWriteOnlyManyEntriesCommand(entries, f, params);
      InvocationContext ctx = fmap.invCtxFactory().createInvocationContext(true, entries.size());
      return futureVoid(futureMode, ctx, cmd);
   }

   @Override
   public CompletableFuture<Void> evalMany(Set<? extends K> keys, Consumer<WriteEntryView<V>> f) {
      log.tracef("Invoked evalMany(keys=%s, %s)", keys, params);
      Param<FutureMode> futureMode = params.get(FutureMode.ID);
      WriteOnlyManyCommand cmd = fmap.cmdFactory().buildWriteOnlyManyCommand(keys, f, params);
      InvocationContext ctx = fmap.invCtxFactory().createInvocationContext(true, keys.size());
      return futureVoid(futureMode, ctx, cmd);
   }

   @Override
   public CompletableFuture<Void> evalAll(Consumer<WriteEntryView<V>> f) {
      log.tracef("Invoked evalAll(%s)", params);
      Param<FutureMode> futureMode = params.get(FutureMode.ID);
      CloseableIteratorSet<K> keys = fmap.cache.keySet();
      WriteOnlyManyCommand cmd = fmap.cmdFactory().buildWriteOnlyManyCommand(keys, f, params);
      InvocationContext ctx = fmap.invCtxFactory().createInvocationContext(true, keys.size());
      return futureVoid(futureMode, ctx, cmd);
   }

   @Override
   public CompletableFuture<Void> truncate() {
      log.tracef("Invoked truncate(%s)", params);
      Param<FutureMode> futureMode = params.get(FutureMode.ID);
      return withFuture(futureMode, fmap.asyncExec(), () -> {
         fmap.cache.clear();
         return null;
      });
   }

   CompletableFuture<Void> futureVoid(Param<FutureMode> futureMode, InvocationContext ctx, VisitableCommand cmd) {
      return withFuture(futureMode, fmap.asyncExec(), () -> {
         fmap.chain().invoke(ctx, cmd);
         return null;
      });
   }

   @Override
   public WriteOnlyMap<K, V> withParams(Param<?>... ps) {
      if (ps == null || ps.length == 0)
         return this;

      if (params.containsAll(ps))
         return this; // We already have all specified params

      return create(params.addAll(ps), fmap);
   }

   @Override
   public WriteListeners<K, V> listeners() {
      return fmap.notifier();
   }

}
