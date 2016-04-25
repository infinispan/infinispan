package org.infinispan.functional.impl;

import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.api.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.commons.api.functional.Listeners.ReadWriteListeners;
import org.infinispan.commons.api.functional.Param;
import org.infinispan.commons.api.functional.Param.FutureMode;
import org.infinispan.commons.api.functional.Traversable;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.infinispan.functional.impl.Params.withFuture;

/**
 * Read-write map implementation.
 *
 * @since 8.0
 */
@Experimental
public final class ReadWriteMapImpl<K, V> extends AbstractFunctionalMap<K, V> implements ReadWriteMap<K, V> {
   private static final Log log = LogFactory.getLog(ReadWriteMapImpl.class);
   private final Params params;

   private ReadWriteMapImpl(Params params, FunctionalMapImpl<K, V> functionalMap) {
      super(functionalMap);
      this.params = params;
   }

   public static <K, V> ReadWriteMap<K, V> create(FunctionalMapImpl<K, V> functionalMap) {
      return new ReadWriteMapImpl<>(Params.from(functionalMap.params.params), functionalMap);
   }

   private static <K, V> ReadWriteMap<K, V> create(Params params, FunctionalMapImpl<K, V> functionalMap) {
      return new ReadWriteMapImpl<>(params, functionalMap);
   }

   @Override
   public <R> CompletableFuture<R> eval(K key, Function<ReadWriteEntryView<K, V>, R> f) {
      log.tracef("Invoked eval(k=%s, %s)", key, params);
      Param<FutureMode> futureMode = params.get(FutureMode.ID);
      ReadWriteKeyCommand cmd = fmap.cmdFactory().buildReadWriteKeyCommand(key, f, params);
      InvocationContext ctx = fmap.invCtxFactory().createInvocationContext(true, 1);
      ctx.setLockOwner(cmd.getKeyLockOwner());
      return withFuture(futureMode, fmap.asyncExec(), () -> (R) fmap.chain().invoke(ctx, cmd));
   }

   @Override
   public <R> CompletableFuture<R> eval(K key, V value, BiFunction<V, ReadWriteEntryView<K, V>, R> f) {
      log.tracef("Invoked eval(k=%s, v=%s, %s)", key, value, params);
      Param<FutureMode> futureMode = params.get(FutureMode.ID);
      ReadWriteKeyValueCommand cmd = fmap.cmdFactory().buildReadWriteKeyValueCommand(key, value, f, params);
      InvocationContext ctx = fmap.invCtxFactory().createInvocationContext(true, 1);
      ctx.setLockOwner(cmd.getKeyLockOwner());
      return withFuture(futureMode, fmap.asyncExec(), () -> (R) fmap.chain().invoke(ctx, cmd));
   }

   @Override
   public <R> Traversable<R> evalMany(Map<? extends K, ? extends V> entries, BiFunction<V, ReadWriteEntryView<K, V>, R> f) {
      log.tracef("Invoked evalMany(entries=%s, %s)", entries, params);
      ReadWriteManyEntriesCommand cmd = fmap.cmdFactory().buildReadWriteManyEntriesCommand(entries, f, params);
      InvocationContext ctx = fmap.invCtxFactory().createInvocationContext(true, entries.size());
      return Traversables.of(((List<R>) fmap.chain().invoke(ctx, cmd)).stream());
   }

   @Override
   public <R> Traversable<R> evalMany(Set<? extends K> keys, Function<ReadWriteEntryView<K, V>, R> f) {
      log.tracef("Invoked evalMany(keys=%s, %s)", keys, params);
      ReadWriteManyCommand cmd = fmap.cmdFactory().buildReadWriteManyCommand(keys, f, params);
      InvocationContext ctx = fmap.invCtxFactory().createInvocationContext(true, keys.size());
      return Traversables.of(((List<R>) fmap.chain().invoke(ctx, cmd)).stream());
   }

   @Override
   public <R> Traversable<R> evalAll(Function<ReadWriteEntryView<K, V>, R> f) {
      log.tracef("Invoked evalAll(%s)", params);
      CloseableIteratorSet<K> keys = fmap.cache.keySet();
      ReadWriteManyCommand cmd = fmap.cmdFactory().buildReadWriteManyCommand(keys, f, params);
      InvocationContext ctx = fmap.invCtxFactory().createInvocationContext(true, keys.size());
      return Traversables.of(((List<R>) fmap.chain().invoke(ctx, cmd)).stream());
   }

   @Override
   public ReadWriteListeners<K, V> listeners() {
      return fmap.notifier();
   }

   @Override
   public ReadWriteMap<K, V> withParams(Param<?>... ps) {
      if (ps == null || ps.length == 0)
         return this;

      if (params.containsAll(ps))
         return this; // We already have all specified params

      return create(params.addAll(ps), fmap);
   }

}
