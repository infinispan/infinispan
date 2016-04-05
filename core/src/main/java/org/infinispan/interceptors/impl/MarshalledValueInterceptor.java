package org.infinispan.interceptors.impl;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.cache.impl.Caches;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.DDSequentialInterceptor;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.stream.impl.interceptor.AbstractDelegatingEntryCacheSet;
import org.infinispan.stream.impl.interceptor.AbstractDelegatingKeyCacheSet;
import org.infinispan.stream.impl.spliterators.IteratorAsSpliterator;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;

import static org.infinispan.factories.KnownComponentNames.CACHE_MARSHALLER;
import static org.infinispan.marshall.core.MarshalledValue.isTypeExcluded;

/**
 * Interceptor that handles the wrapping and unwrapping of cached data using {@link
 * MarshalledValue}s. Known "excluded" types are not wrapped/unwrapped, which at this time
 * include {@link String}, Java primitives and their Object wrappers, as well as arrays of excluded types.
 * <p/>
 * The {@link MarshalledValue} wrapper handles lazy deserialization from byte array
 * representations.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @see MarshalledValue
 * @since 9.0
 */
public class MarshalledValueInterceptor<K, V> extends DDSequentialInterceptor {
   private StreamingMarshaller marshaller;
   private boolean wrapKeys = true;
   private boolean wrapValues = true;
   private InternalEntryFactory entryFactory;
   private Cache<K, V> cache;

   private static final Log log = LogFactory.getLog(MarshalledValueInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject
   protected void inject(@ComponentName(CACHE_MARSHALLER) StreamingMarshaller marshaller,
                         InternalEntryFactory entryFactory, Cache<K, V> cache) {
      this.marshaller = marshaller;
      this.entryFactory = entryFactory;
      this.cache = cache;
   }

   @Start
   protected void start() {
      wrapKeys = cacheConfiguration.storeAsBinary().storeKeysAsBinary();
      wrapValues = cacheConfiguration.storeAsBinary().storeValuesAsBinary();
   }

   @Override
   public CompletableFuture<Void> visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (wrapKeys) {
         if (command.multipleKeys()) {
            Collection<Object> rawKeys = command.getKeys();
            Map<Object, Object> keyToMarshalledKeyMapping = new HashMap<Object, Object>(rawKeys.size());
            for (Object k : rawKeys) {
               if (!isTypeExcluded(k.getClass())) keyToMarshalledKeyMapping.put(k, createMarshalledValue(k, ctx));
            }

            if (!keyToMarshalledKeyMapping.isEmpty()) command.replaceKeys(keyToMarshalledKeyMapping);
         } else {
            Object key = command.getSingleKey();
            if (!isTypeExcluded(key.getClass())) command.replaceKey(key, createMarshalledValue(key, ctx));
         }
      }

      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Set<MarshalledValue> marshalledValues = new HashSet<MarshalledValue>(command.getMap().size());
      Map<Object, Object> map = wrapMap(command.getMap(), marshalledValues, ctx);
      command.setMap(map);
      Object retVal = ctx.forkInvocationSync(command);
      return ctx.shortCircuit(processRetVal(retVal, ctx));
   }

   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      MarshalledValue key;
      MarshalledValue value;
      if (wrapKeys) {
         if (!isTypeExcluded(command.getKey().getClass())) {
            key = createMarshalledValue(command.getKey(), ctx);
            command.setKey(key);
         }
      }

      if (wrapValues) {
         if (!isTypeExcluded(command.getValue().getClass())) {
            value = createMarshalledValue(command.getValue(), ctx);
            command.setValue(value);
         }
      }

      Object retVal = ctx.forkInvocationSync(command);
      return ctx.shortCircuit(processRetVal(retVal, ctx));
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      MarshalledValue value;
      if (wrapKeys) {
         if (!isTypeExcluded(command.getKey().getClass())) {
            value = createMarshalledValue(command.getKey(), ctx);
            command.setKey(value);
         }
      }
      Object retVal = ctx.forkInvocationSync(command);
      return ctx.shortCircuit(processRetVal(retVal, ctx));
   }

   @Override
   public CompletableFuture<Void> visitEvictCommand(InvocationContext ctx, org.infinispan.commands.write.EvictCommand command) throws Throwable {
      MarshalledValue value;
      if (wrapKeys) {
         if (!isTypeExcluded(command.getKey().getClass())) {
            value = createMarshalledValue(command.getKey(), ctx);
            command.setKey(value);
         }
      }
      Object retVal = ctx.forkInvocationSync(command);
      return ctx.shortCircuit(processRetVal(retVal, ctx));
   }

   @Override
   public final CompletableFuture<Void> visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }
   @Override
   public final CompletableFuture<Void> visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }
   private CompletableFuture<Void> visitDataReadCommand(InvocationContext ctx, AbstractDataCommand command) throws Throwable {
      MarshalledValue mv;
      if (wrapKeys) {
         if (!isTypeExcluded(command.getKey().getClass())) {
            mv = createMarshalledValue(command.getKey(), ctx);
            command.setKey(mv);
         }
      }
      Object retVal = ctx.forkInvocationSync(command);
      return ctx.shortCircuit(processRetVal(retVal, ctx));
   }

   @Override
   public CompletableFuture<Void> visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      if (wrapKeys) {
         Set<Object> marshalledKeys = new LinkedHashSet<>();
         for (Object key : command.getKeys()) {
            if (!isTypeExcluded(key.getClass())) {
               MarshalledValue mv = createMarshalledValue(key, ctx);
               marshalledKeys.add(mv);
            } else {
               marshalledKeys.add(key);
            }
         }
         command.setKeys(marshalledKeys);
      }
      Map<Object, Object> map = (Map<Object, Object>) ctx.forkInvocationSync(command);
      Map<Object, Object> unmarshalled = command.createMap();
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
         // TODO: how does this apply to CacheEntries if command.isReturnEntries()?
         unmarshalled.put(processRetVal(entry.getKey(), ctx), processRetVal(entry.getValue(), ctx));
      }
      return ctx.shortCircuit(unmarshalled);
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      MarshalledValue key, newValue, oldValue;
      if (wrapKeys && !isTypeExcluded(command.getKey().getClass())) {
         key = createMarshalledValue(command.getKey(), ctx);
         command.setKey(key);
      }
      if (wrapValues && !isTypeExcluded(command.getNewValue().getClass())) {
         newValue = createMarshalledValue(command.getNewValue(), ctx);
         command.setNewValue(newValue);
      }
      if (wrapValues && command.getOldValue() != null && !isTypeExcluded(command.getOldValue().getClass())) {
         oldValue = createMarshalledValue(command.getOldValue(), ctx);
         command.setOldValue(oldValue);
      }
      Object retVal = ctx.forkInvocationSync(command);
      return ctx.shortCircuit(processRetVal(retVal, ctx));
   }

   protected <R> R processRetVal(R retVal, InvocationContext ctx) {
      if (retVal instanceof MarshalledValue) {
         if (ctx == null || ctx.isOriginLocal()) {
            if (trace) log.tracef("Return is a marshall value, so extract instance from: %s", retVal);
            retVal = (R) ((MarshalledValue) retVal).get();
         }
      }
      return retVal;
   }

   private final CacheEntry<K, V> unwrapEntry(CacheEntry<K, V> e, InvocationContext ctx) {
      Object originalKey = e.getKey();
      Object key = processRetVal(originalKey, ctx);
      Object originalValue = e.getValue();
      Object value = processRetVal(originalValue, ctx);
      if (originalKey != key || originalValue != value) {
         return (CacheEntry<K, V>) entryFactory.create(key, value, e.getMetadata());
      }
      return e;
   }

   @Override
   public CompletableFuture<Void> visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      CacheSet<CacheEntry<K, V>> set = (CacheSet<CacheEntry<K, V>>) ctx.forkInvocationSync(command);

      return ctx.shortCircuit(new AbstractDelegatingEntryCacheSet<K, V>(Caches.getCacheWithFlags(cache, command), set) {
         @Override
         public CloseableIterator<CacheEntry<K, V>> iterator() {
            // We pass a null ctx, since we always want this value unwrapped.  If iterator was invoked locally, it would
            // behave the same, however for a remote invocation which is usually part of a stream invocation we have
            // to have the actual unmarshalled value to perform the intermediate operations upon.
            return new CloseableIteratorMapper<>(super.iterator(), e -> unwrapEntry(e, null));
         }

         @Override
         public CloseableSpliterator<CacheEntry<K, V>> spliterator() {
            return new IteratorAsSpliterator.Builder<>(iterator())
                    .setEstimateRemaining(super.spliterator().estimateSize())
                    .setCharacteristics(Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL)
                    .get();
         }
      });
   }

   @Override
   public CompletableFuture<Void> visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      CacheSet<K> set = (CacheSet<K>) ctx.forkInvocationSync(command);

      return ctx.shortCircuit(new AbstractDelegatingKeyCacheSet<K, V>(Caches.getCacheWithFlags(cache, command), set) {

         @Override
         public CloseableIterator<K> iterator() {
            // We pass a null ctx, since this is always local invocation - if it was remote it would use
            // DistributionBulkInterceptor
            return new CloseableIteratorMapper<>(super.iterator(), e -> processRetVal(e, null));
         }

         @Override
         public CloseableSpliterator<K> spliterator() {
            return new IteratorAsSpliterator.Builder<>(iterator())
                    .setEstimateRemaining(super.spliterator().estimateSize())
                    .setCharacteristics(Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL)
                    .get();
         }
      });
   }

   @SuppressWarnings("unchecked")
   protected Map<Object, Object> wrapMap(Map<Object, Object> m, Set<MarshalledValue> marshalledValues, InvocationContext ctx) {
      if (m == null) {
         if (trace) log.trace("Map is nul; returning an empty map.");
         return Collections.emptyMap();
      }
      if (trace) log.tracef("Wrapping map contents of argument %s", m);
      Map<Object, Object> copy = new HashMap(m.size());
      for (Map.Entry<Object, Object> me : m.entrySet()) {
         Object key = me.getKey();
         Object value = me.getValue();
         Object newKey = (key == null || isTypeExcluded(key.getClass())) || !wrapKeys ? key : createMarshalledValue(key, ctx);
         Object newValue = (value == null || isTypeExcluded(value.getClass()) || !wrapValues) ? value : createMarshalledValue(value, ctx);
         if (newKey instanceof MarshalledValue) marshalledValues.add((MarshalledValue) newKey);
         if (newValue instanceof MarshalledValue) marshalledValues.add((MarshalledValue) newValue);
         copy.put(newKey, newValue);
      }
      return copy;
   }

   protected MarshalledValue createMarshalledValue(Object toWrap, InvocationContext ctx) {
      return new MarshalledValue(toWrap, marshaller);
   }
}
