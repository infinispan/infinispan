package org.infinispan.functional.impl;

import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commons.util.Experimental;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.FunctionalMap.ReadOnlyMap;
import org.infinispan.functional.Param;
import org.infinispan.functional.Traversable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Read-only map implementation.
 *
 * @since 8.0
 */
@Experimental
public final class ReadOnlyMapImpl<K, V> extends AbstractFunctionalMap<K, V> implements ReadOnlyMap<K, V> {
   private static final Log log = LogFactory.getLog(ReadOnlyMapImpl.class);

   private ReadOnlyMapImpl(Params params, FunctionalMapImpl<K, V> functionalMap) {
      super(params, functionalMap);
   }

   public static <K, V> ReadOnlyMap<K, V> create(FunctionalMapImpl<K, V> functionalMap) {
      return create(Params.from(functionalMap.params.params), functionalMap);
   }

   private static <K, V> ReadOnlyMap<K, V> create(Params params, FunctionalMapImpl<K, V> functionalMap) {
      return new ReadOnlyMapImpl<>(params, functionalMap);
   }

   @Override
   public <R> CompletableFuture<R> eval(K key, Function<ReadEntryView<K, V>, R> f) {
      log.tracef("Invoked eval(k=%s, %s)", key, params);
      Object keyEncoded = keyDataConversion.toStorage(key);
      ReadOnlyKeyCommand<K, V, R> cmd = fmap.commandsFactory.buildReadOnlyKeyCommand(keyEncoded, f,
            fmap.keyPartitioner.getSegment(keyEncoded), params, keyDataConversion, valueDataConversion);
      InvocationContext ctx = fmap.invCtxFactory.createInvocationContext(false, 1);
      return (CompletableFuture<R>) fmap.chain.invokeAsync(ctx, cmd);
   }

   @Override
   public <R> Traversable<R> evalMany(Set<? extends K> keys, Function<ReadEntryView<K, V>, R> f) {
      log.tracef("Invoked evalMany(m=%s, %s)", keys, params);
      Set<?> encodedKeys = encodeKeys(keys);
      ReadOnlyManyCommand<K, V, R> cmd = fmap.commandsFactory.buildReadOnlyManyCommand(encodedKeys, f, params, keyDataConversion, valueDataConversion);
      InvocationContext ctx = fmap.invCtxFactory.createInvocationContext(false, keys.size());
      return Traversables.of((Stream<R>) fmap.chain.invokeAsync(ctx, cmd).join());
   }

   @Override
   public Traversable<K> keys() {
      log.tracef("Invoked keys(%s)", params);
      return Traversables.of(fmap.cache.keySet().stream());
   }

   @Override
   public Traversable<ReadEntryView<K, V>> entries() {
      log.tracef("Invoked entries(%s)", params);
      Iterator<CacheEntry<K, V>> it = fmap.cache.cacheEntrySet().iterator();
      // TODO: Don't really need a Stream here...
      Stream<CacheEntry<K, V>> stream = StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(it, Spliterator.IMMUTABLE), false);
      return Traversables.of(stream.map(EntryViews::readOnly));
   }

   @Override
   public ReadOnlyMap<K, V> withParams(Param<?>... ps) {
      if (ps == null || ps.length == 0)
         return this;

      if (params.containsAll(ps))
         return this; // We already have all specified params

      return create(params.addAll(ps), fmap);
   }

}
