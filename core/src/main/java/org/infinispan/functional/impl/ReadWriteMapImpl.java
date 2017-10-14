package org.infinispan.functional.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.Listeners.ReadWriteListeners;
import org.infinispan.functional.Param;
import org.infinispan.functional.Traversable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Read-write map implementation.
 *
 * @since 8.0
 */
@Experimental
public final class ReadWriteMapImpl<K, V> extends AbstractFunctionalMap<K, V> implements ReadWriteMap<K, V> {
   private static final Log log = LogFactory.getLog(ReadWriteMapImpl.class);

   private ReadWriteMapImpl(Params params, FunctionalMapImpl<K, V> functionalMap) {
      super(params, functionalMap);
   }

   public static <K, V> ReadWriteMap<K, V> create(FunctionalMapImpl<K, V> functionalMap) {
      return create(Params.from(functionalMap.params.params), functionalMap);
   }

   private static <K, V> ReadWriteMap<K, V> create(Params params, FunctionalMapImpl<K, V> functionalMap) {
      return new ReadWriteMapImpl<>(params, functionalMap);
   }

   @Override
   public <R> CompletableFuture<R> eval(K key, Function<ReadWriteEntryView<K, V>, R> f) {
      log.tracef("Invoked eval(k=%s, %s)", key, params);
      Object keyEncoded = keyDataConversion.toStorage(key);
      ReadWriteKeyCommand<K, V, R> cmd = fmap.commandsFactory.buildReadWriteKeyCommand(keyEncoded, (Function) f, params, keyDataConversion, valueDataConversion);
      InvocationContext ctx = getInvocationContext(true, 1);
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return invokeAsync(ctx, cmd);
   }

   @Override
   public <R> CompletableFuture<R> eval(K key, V value, BiFunction<V, ReadWriteEntryView<K, V>, R> f) {
      log.tracef("Invoked eval(k=%s, v=%s, %s)", key, value, params);
      Object keyEncoded = keyDataConversion.toStorage(key);
      Object valueEncoded = valueDataConversion.toStorage(value);
      ReadWriteKeyValueCommand<K, V, R> cmd = fmap.commandsFactory.buildReadWriteKeyValueCommand(keyEncoded, valueEncoded, (BiFunction) f, params, keyDataConversion, valueDataConversion);
      InvocationContext ctx = getInvocationContext(true, 1);
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return invokeAsync(ctx, cmd);
   }

   @Override
   public <R> Traversable<R> evalMany(Map<? extends K, ? extends V> entries, BiFunction<V, ReadWriteEntryView<K, V>, R> f) {
      log.tracef("Invoked evalMany(entries=%s, %s)", entries, params);
      Map<?, ?> encodedEntries = encodeEntries(entries);
      ReadWriteManyEntriesCommand<K, V, R> cmd = fmap.commandsFactory.buildReadWriteManyEntriesCommand(encodedEntries, f, params, keyDataConversion, valueDataConversion);
      InvocationContext ctx = getInvocationContext(true, entries.size());
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return Traversables.of(((List<R>) invokeAsync(ctx, cmd).join()).stream());
   }

   @Override
   public <R> Traversable<R> evalMany(Set<? extends K> keys, Function<ReadWriteEntryView<K, V>, R> f) {
      log.tracef("Invoked evalMany(keys=%s, %s)", keys, params);
      Set<?> encodedKeys = encodeKeys(keys);
      ReadWriteManyCommand<K, V, R> cmd = fmap.commandsFactory.buildReadWriteManyCommand(encodedKeys, f, params, keyDataConversion, valueDataConversion);
      InvocationContext ctx = getInvocationContext(true, keys.size());
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return Traversables.of(((List<R>) invokeAsync(ctx, cmd).join()).stream());
   }

   @Override
   public <R> Traversable<R> evalAll(Function<ReadWriteEntryView<K, V>, R> f) {
      log.tracef("Invoked evalAll(%s)", params);
      // TODO: during commmand execution the set is iterated multiple times, and can execute remote operations
      // therefore we should rather have separate command (or different semantics for keys == null)
      Set<K> keys = new HashSet<>(fmap.cache.keySet());
      Set<?> encodedKeys = encodeKeys(keys);
      ReadWriteManyCommand<K, V, R> cmd = fmap.commandsFactory.buildReadWriteManyCommand(encodedKeys, f, params, keyDataConversion, valueDataConversion);
      InvocationContext ctx = getInvocationContext(true, encodedKeys.size());
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return Traversables.of(((List<R>) invokeAsync(ctx, cmd).join()).stream());
   }

   @Override
   public ReadWriteListeners<K, V> listeners() {
      return fmap.notifier;
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
