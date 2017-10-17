package org.infinispan.functional.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.functional.Listeners.WriteListeners;
import org.infinispan.functional.Param;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Write-only map implementation.
 *
 * @since 8.0
 */
@Experimental
public final class WriteOnlyMapImpl<K, V> extends AbstractFunctionalMap<K, V> implements WriteOnlyMap<K, V> {
   private static final Log log = LogFactory.getLog(WriteOnlyMapImpl.class);

   private WriteOnlyMapImpl(Params params, FunctionalMapImpl<K, V> functionalMap) {
      super(params, functionalMap);
   }

   public static <K, V> WriteOnlyMap<K, V> create(FunctionalMapImpl<K, V> functionalMap) {
      return create(Params.from(functionalMap.params.params), functionalMap);
   }

   private static <K, V> WriteOnlyMap<K, V> create(Params params, FunctionalMapImpl<K, V> functionalMap) {
      return new WriteOnlyMapImpl<>(params, functionalMap);
   }

   @Override
   public CompletableFuture<Void> eval(K key, Consumer<WriteEntryView<K, V>> f) {
      log.tracef("Invoked eval(k=%s, %s)", key, params);
      Object keyEncoded = keyDataConversion.toStorage(key);
      WriteOnlyKeyCommand<K, V> cmd = fmap.commandsFactory.buildWriteOnlyKeyCommand(keyEncoded, f, params, keyDataConversion, valueDataConversion);
      InvocationContext ctx = getInvocationContext(true, 1);
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return invokeAsync(ctx, cmd);
   }

   @Override
   public <T> CompletableFuture<Void> eval(K key, T argument, BiConsumer<T, WriteEntryView<K, V>> f) {
      log.tracef("Invoked eval(k=%s, v=%s, %s)", key, argument, params);
      Object keyEncoded = keyDataConversion.toStorage(key);
      Object argumentEncoded = valueDataConversion.toStorage(argument);
      WriteOnlyKeyValueCommand<K, V, T> cmd = fmap.commandsFactory.buildWriteOnlyKeyValueCommand(keyEncoded, argumentEncoded, (BiConsumer) f, params, keyDataConversion, valueDataConversion);
      InvocationContext ctx = getInvocationContext(true, 1);
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return invokeAsync(ctx, cmd);
   }

   @Override
   public <T> CompletableFuture<Void> evalMany(Map<? extends K, ? extends T> arguments, BiConsumer<T, WriteEntryView<K, V>> f) {
      log.tracef("Invoked evalMany(entries=%s, %s)", arguments, params);
      Map<?, ?> argumentsEncoded = encodeEntries(arguments);
      WriteOnlyManyEntriesCommand<K, V, T> cmd = fmap.commandsFactory.buildWriteOnlyManyEntriesCommand(argumentsEncoded, f, params, keyDataConversion, valueDataConversion);
      InvocationContext ctx = getInvocationContext(true, arguments.size());
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return invokeAsync(ctx, cmd);
   }

   @Override
   public CompletableFuture<Void> evalMany(Set<? extends K> keys, Consumer<WriteEntryView<K, V>> f) {
      log.tracef("Invoked evalMany(keys=%s, %s)", keys, params);
      Set<?> encodedKeys = encodeKeys(keys);
      WriteOnlyManyCommand<K, V> cmd = fmap.commandsFactory.buildWriteOnlyManyCommand(encodedKeys, f, params, keyDataConversion, valueDataConversion);
      InvocationContext ctx = getInvocationContext(true, keys.size());
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return invokeAsync(ctx, cmd);
   }

   @Override
   public CompletableFuture<Void> evalAll(Consumer<WriteEntryView<K, V>> f) {
      log.tracef("Invoked evalAll(%s)", params);
      // TODO: during commmand execution the set is iterated multiple times, and can execute remote operations
      // therefore we should rather have separate command (or different semantics for keys == null)
      Set<K> keys = new HashSet<>(fmap.cache.keySet());
      Set<?> encodedKeys = encodeKeys(keys);
      WriteOnlyManyCommand<K, V> cmd = fmap.commandsFactory.buildWriteOnlyManyCommand(encodedKeys, f, params, keyDataConversion, valueDataConversion);
      InvocationContext ctx = getInvocationContext(true, encodedKeys.size());
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return invokeAsync(ctx, cmd);
   }

   @Override
   public CompletableFuture<Void> truncate() {
      log.tracef("Invoked truncate(%s)", params);
      return fmap.cache.clearAsync();
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
      return fmap.notifier;
   }

}
