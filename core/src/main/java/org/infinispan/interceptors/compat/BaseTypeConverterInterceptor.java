package org.infinispan.interceptors.compat;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.compat.TypeConverter;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.metadata.Metadata;
import org.infinispan.stream.impl.interceptor.AbstractDelegatingEntryCacheSet;
import org.infinispan.stream.impl.interceptor.AbstractDelegatingKeyCacheSet;
import org.infinispan.stream.impl.local.EntryStreamSupplier;
import org.infinispan.stream.impl.local.KeyStreamSupplier;
import org.infinispan.stream.impl.local.LocalCacheStream;
import org.infinispan.stream.impl.spliterators.IteratorAsSpliterator;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.StreamSupport;

/**
 * Base implementation for an interceptor that applies type conversion to the data stored in the cache. Subclasses need
 * to provide a suitable TypeConverter.
 *
 * @author Galder Zamarreño
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public abstract class BaseTypeConverterInterceptor<K, V> extends CommandInterceptor {

   private InternalEntryFactory entryFactory;
   private VersionGenerator versionGenerator;
   private Cache<K, V> cache;


   @Inject
   protected void init(InternalEntryFactory entryFactory, VersionGenerator versionGenerator, Cache<K, V> cache) {
      this.entryFactory = entryFactory;
      this.versionGenerator = versionGenerator;
      this.cache = cache;
   }

   /**
    * Subclasses need to return a TypeConverter instance that is appropriate for a cache operation with the specified flags.
    *
    * @param flags the set of flags for the current cache operation
    * @return the converter, never {@code null}
    */
   protected abstract TypeConverter<Object, Object, Object, Object> determineTypeConverter(Set<Flag> flags);

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      TypeConverter<Object, Object, Object, Object> converter =
            determineTypeConverter(command.getFlags());
      if (ctx.isOriginLocal()) {
         command.setKey(converter.boxKey(key));
         command.setValue(converter.boxValue(command.getValue()));
      }
      Object ret = invokeNextInterceptor(ctx, command);
      return converter.unboxValue(ret);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         Map<Object, Object> map = command.getMap();
         TypeConverter<Object, Object, Object, Object> converter =
               determineTypeConverter(command.getFlags());
         Map<Object, Object> convertedMap = new HashMap<>(map.size());
         for (Entry<Object, Object> entry : map.entrySet()) {
            convertedMap.put(converter.boxKey(entry.getKey()),
                  converter.boxValue(entry.getValue()));
         }
         command.setMap(convertedMap);
      }
      // There is no return value for putAll so nothing to convert
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      TypeConverter<Object, Object, Object, Object> converter =
            determineTypeConverter(command.getFlags());
      if (ctx.isOriginLocal()) {
         command.setKey(converter.boxKey(key));
      }
      Object ret = invokeNextInterceptor(ctx, command);
      if (ret != null) {
         if (needsUnboxing(ctx)) {
            return converter.unboxValue(ret);
         }
         return ret;
      }
      return null;
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      Object key = command.getKey();
      TypeConverter<Object, Object, Object, Object> converter =
            determineTypeConverter(command.getFlags());
      if (ctx.isOriginLocal()) {
         command.setKey(converter.boxKey(key));
      }
      Object ret = invokeNextInterceptor(ctx, command);
      if (ret != null) {
         CacheEntry entry = (CacheEntry) ret;
         Object returnValue = entry.getValue();
         if (needsUnboxing(ctx)) {
            returnValue = converter.unboxValue(entry.getValue());
         }
         // Create a copy of the entry to avoid modifying the internal entry
         return entryFactory.create(
               entry.getKey(), returnValue, entry.getMetadata(),
               entry.getLifespan(), entry.getMaxIdle());
      }
      return null;
   }

   private boolean needsUnboxing(InvocationContext ctx) {
      return ctx.isOriginLocal();
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      Collection<?> keys = command.getKeys();
      TypeConverter<Object, Object, Object, Object> converter =
            determineTypeConverter(command.getFlags());
      if (ctx.isOriginLocal()) {
         Set<Object> boxedKeys = new LinkedHashSet<>(keys.size());
         for (Object key : keys) {
            boxedKeys.add(converter.boxKey(key));
         }
         command.setKeys(boxedKeys);
      }
      Object ret = invokeNextInterceptor(ctx, command);

      if (ret != null && !needsUnboxing(ctx))
         return ret;

      if (ret != null) {
         if (command.isReturnEntries()) {
            Map<Object, CacheEntry> map = (Map<Object, CacheEntry>) ret;
            Map<Object, Object> unboxed = command.createMap();
            for (Map.Entry<Object, CacheEntry> entry : map.entrySet()) {
               CacheEntry cacheEntry = entry.getValue();
               if (cacheEntry == null) {
                  unboxed.put(entry.getKey(), null);
               } else {
                  if (command.getRemotelyFetched() == null || !command.getRemotelyFetched().containsKey(entry.getKey())) {
                     unboxed.put(converter.unboxKey(entry.getKey()), entryFactory.create(entry.getKey(),
                           converter.unboxValue(cacheEntry.getValue()),
                           cacheEntry.getMetadata(), cacheEntry.getLifespan(), cacheEntry.getMaxIdle()));
                  } else {
                     unboxed.put(converter.unboxKey(entry.getKey()), cacheEntry);
                  }
               }
            }
            return unboxed;
         } else {
            Map<Object, Object> map = (Map<Object, Object>) ret;
            Map<Object, Object> unboxed = command.createMap();
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
               Object value = entry == null ? null : entry.getValue();
               unboxed.put(converter.unboxKey(entry.getKey()), entry == null ? null : converter.unboxValue(value));
            }
            return unboxed;
         }
      }

      return null;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object key = command.getKey();
      TypeConverter<Object, Object, Object, Object> converter =
            determineTypeConverter(command.getFlags());
      Object oldValue = command.getOldValue();
      if (ctx.isOriginLocal()) {
         command.setKey(converter.boxKey(key));
         command.setOldValue(converter.boxValue(oldValue));
         command.setNewValue(converter.boxValue(command.getNewValue()));
      }
      addVersionIfNeeded(command);
      Object ret = invokeNextInterceptor(ctx, command);

      // Return of conditional replace is not the value type, but boolean, so
      // apply an exception that applies to all servers, regardless of what's
      // stored in the value side
      if (oldValue != null && ret instanceof Boolean)
         return ret;

      return converter.unboxValue(ret);
   }

   private void addVersionIfNeeded(MetadataAwareCommand cmd) {
      Metadata metadata = cmd.getMetadata();
      if (metadata.version() == null) {
         Metadata newMetadata = metadata.builder()
               .version(versionGenerator.generateNew())
               .build();
         cmd.setMetadata(newMetadata);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object key = command.getKey();
      TypeConverter<Object, Object, Object, Object> converter =
            determineTypeConverter(command.getFlags());
      Object conditionalValue = command.getValue();
      if (ctx.isOriginLocal()) {
         command.setKey(converter.boxKey(key));
         command.setValue(converter.boxValue(conditionalValue));
      }
      Object ret = invokeNextInterceptor(ctx, command);

      // Return of conditional remove is not the value type, but boolean, so
      // apply an exception that applies to all servers, regardless of what's
      // stored in the value side
      if (conditionalValue != null && ret instanceof Boolean)
         return ret;

      return ctx.isOriginLocal() ? converter.unboxValue(ret) : ret;
   }

   @Override
   public CacheSet<K> visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         TypeConverter<Object, Object, Object, Object> converter = determineTypeConverter(command.getFlags());
         CacheSet<K> set = (CacheSet<K>) super.visitKeySetCommand(ctx, command);

         return new AbstractDelegatingKeyCacheSet<K, V>(getCacheWithFlags(cache, command), set) {

            @Override
            public CloseableIterator<K> iterator() {
               return new CloseableIteratorMapper<>(super.iterator(), k -> (K) converter.unboxKey(k));
            }

            @Override
            public CloseableSpliterator<K> spliterator() {
               return new IteratorAsSpliterator.Builder<>(iterator())
                       .setEstimateRemaining(super.spliterator().estimateSize())
                       .setCharacteristics(Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL)
                       .get();
            }

            protected CacheStream<K> getStream(boolean parallel) {
               DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
               // Note the stream has to deal with the boxed values - so we can't use our spliterator as it already
               // unboxes them
               CloseableSpliterator<K> closeableSpliterator = super.spliterator();
               CacheStream<K> stream = new LocalCacheStream<>(new KeyStreamSupplier<>(cache,
                       dm != null ? dm.getConsistentHash() : null,
                       () -> StreamSupport.stream(closeableSpliterator, parallel)), parallel,
                       cache.getAdvancedCache().getComponentRegistry());
               // We rely on the fact that on close returns the same instance
               stream.onClose(closeableSpliterator::close);
               return new TypeConverterStream(stream, converter, entryFactory);
            }
         };
      }
      return (CacheSet<K>) invokeNextInterceptor(ctx, command);
   }

   @Override
   public CacheSet<CacheEntry<K, V>> visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         TypeConverter<Object, Object, Object, Object> converter = determineTypeConverter(command.getFlags());
         CacheSet<CacheEntry<K, V>> set = (CacheSet<CacheEntry<K, V>>) super.visitEntrySetCommand(ctx, command);

         return new AbstractDelegatingEntryCacheSet<K, V>(getCacheWithFlags(cache, command), set) {
            @Override
            public CloseableIterator<CacheEntry<K, V>> iterator() {
               return new TypeConverterIterator<>(super.iterator(), converter, entryFactory);
            }

            @Override
            public CloseableSpliterator<CacheEntry<K, V>> spliterator() {
               return new IteratorAsSpliterator.Builder<>(iterator())
                       .setEstimateRemaining(super.spliterator().estimateSize())
                       .setCharacteristics(Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL)
                       .get();
            }

            protected CacheStream<CacheEntry<K, V>> getStream(boolean parallel) {
               DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
               // Note the stream has to deal with the boxed values - so we can't use our spliterator as it already
               // unboxes them
               CloseableSpliterator<CacheEntry<K, V>> closeableSpliterator = super.spliterator();
               CacheStream<CacheEntry<K, V>> stream = new LocalCacheStream<>(new EntryStreamSupplier<>(cache,
                       dm != null ? dm.getConsistentHash() : null,
                       () -> StreamSupport.stream(closeableSpliterator, parallel)), parallel,
                       cache.getAdvancedCache().getComponentRegistry());
               // We rely on the fact that on close returns the same instance
               stream.onClose(closeableSpliterator::close);
               return new TypeConverterStream(stream, converter, entryFactory);
            }
         };
      }
      return (CacheSet<CacheEntry<K, V>>) invokeNextInterceptor(ctx, command);
   }

   private static <K, V> CacheEntry<K, V> convert(CacheEntry<K, V> entry,
           TypeConverter<Object, Object, Object, Object> converter, InternalEntryFactory entryFactory) {
      K newKey = (K) converter.unboxKey(entry.getKey());
      V newValue = (V) converter.unboxValue(entry.getValue());
      // If either value changed then make a copy
      if (newKey != entry.getKey() || newValue != entry.getValue()) {
         return entryFactory.create(newKey, newValue, entry.getMetadata());
      }
      return entry;
   }

   private static class TypeConverterIterator<K, V> implements CloseableIterator<CacheEntry<K, V>> {
      private final CloseableIterator<CacheEntry<K, V>> iterator;
      private final TypeConverter<Object, Object, Object, Object> converter;
      private final InternalEntryFactory entryFactory;

      private TypeConverterIterator(CloseableIterator<CacheEntry<K, V>> iterator,
                                    TypeConverter<Object, Object, Object, Object> converter,
                                    InternalEntryFactory entryFactory) {
         this.iterator = iterator;
         this.converter = converter;
         this.entryFactory = entryFactory;
      }

      @Override
      public void close() {
         iterator.close();
      }

      @Override
      public boolean hasNext() {
         return iterator.hasNext();
      }

      @Override
      public CacheEntry<K, V> next() {
         CacheEntry<K, V> entry = iterator.next();
         return convert(entry, converter, entryFactory);
      }

      @Override
      public void remove() {
         iterator.remove();
      }
   }
}
