package org.infinispan.functional.impl;

import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.FunctionalMap.ReadOnlyMap;
import org.infinispan.commons.api.functional.Param;
import org.infinispan.commons.api.functional.Param.WaitMode;
import org.infinispan.commons.api.functional.Traversable;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.infinispan.functional.impl.WaitModes.withWaitFuture;
import static org.infinispan.functional.impl.WaitModes.withWaitTraversable;

public final class ReadOnlyMapImpl<K, V> extends AbstractFunctionalMap<K, V> implements ReadOnlyMap<K, V> {
   private static final Log log = LogFactory.getLog(ReadOnlyMapImpl.class);
   private final Params params;

   private ReadOnlyMapImpl(Params params, FunctionalMapImpl<K, V> functionalMap) {
      super(functionalMap);
      this.params = params;
   }

   public static <K, V> ReadOnlyMap<K, V> create(FunctionalMapImpl<K, V> functionalMap) {
      return new ReadOnlyMapImpl<>(Params.from(functionalMap.params.params), functionalMap);
   }

   private static <K, V> ReadOnlyMap<K, V> create(Params params, FunctionalMapImpl<K, V> functionalMap) {
      return new ReadOnlyMapImpl<>(params, functionalMap);
   }

   @Override
   public <R> CompletableFuture<R> eval(K key, Function<ReadEntryView<K, V>, R> f) {
      log.tracef("Invoked eval(k=%s, %s)%n", key, params);
      Param<WaitMode> waitMode = params.get(WaitMode.ID);
      ReadOnlyKeyCommand cmd = fmap.cmdFactory().buildReadOnlyKeyCommand(key, f);
      InvocationContext ctx = fmap.invCtxFactory().createInvocationContext(false, 1);
      return withWaitFuture(waitMode, fmap.asyncExec(), () -> (R) fmap.chain().invoke(ctx, cmd));
   }

   @Override
   public <R> Traversable<R> evalMany(Set<? extends K> keys, Function<ReadEntryView<K, V>, R> f) {
      log.tracef("Invoked evalMany(m=%s, %s)%n", keys, params);
      Param<WaitMode> waitMode = params.get(WaitMode.ID);
      ReadOnlyManyCommand<K, V, R> cmd = fmap.cmdFactory().buildReadOnlyManyCommand(keys, f);
      InvocationContext ctx = fmap.invCtxFactory().createInvocationContext(false, keys.size());
      return withWaitTraversable(waitMode, () -> (Stream<R>) fmap.chain().invoke(ctx, cmd));
   }

   @Override
   public Traversable<K> keys() {
      log.tracef("Invoked keys(%s)%n", params);
      Param<WaitMode> waitMode = params.get(WaitMode.ID);
      return withWaitTraversable(waitMode, () -> fmap.cache.keySet().stream());
   }

   @Override
   public Traversable<ReadEntryView<K, V>> entries() {
      log.tracef("Invoked entries(%s)%n", params);
      Param<WaitMode> waitMode = params.get(WaitMode.ID);
      CloseableIterator<CacheEntry<K, V>> it = fmap.cache
            .filterEntries(AcceptAllKeyValueFilter.getInstance()).iterator();
      // TODO: Don't really need a Stream here...
      Stream<CacheEntry<K, V>> stream = StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(it, Spliterator.IMMUTABLE), false);
      return withWaitTraversable(waitMode, () -> stream.map(EntryViews::readOnly));
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
