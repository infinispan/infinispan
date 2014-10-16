package org.infinispan.interceptors.compat;

import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.read.EntryRetrievalCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.compat.TypeConverter;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.Converter;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.iteration.EntryIterable;
import org.infinispan.metadata.Metadata;

import java.util.Set;

/**
 * Base implementation for an interceptor that applies type conversion to the data stored in the cache. Subclasses need
 * to provide a suitable TypeConverter.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public abstract class BaseTypeConverterInterceptor extends CommandInterceptor {

   private InternalEntryFactory entryFactory;
   private VersionGenerator versionGenerator;


   @Inject
   protected void init(InternalEntryFactory entryFactory, VersionGenerator versionGenerator) {
      this.entryFactory = entryFactory;
      this.versionGenerator = versionGenerator;
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
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      TypeConverter<Object, Object, Object, Object> converter =
            determineTypeConverter(command.getFlags());
      if (ctx.isOriginLocal()) {
         command.setKey(converter.boxKey(key));
      }
      Object ret = invokeNextInterceptor(ctx, command);
      if (ret != null) {
         if (command.isReturnEntry()) {
            CacheEntry entry = (CacheEntry) ret;
            Object returnValue = entry.getValue();
            if (command.getRemotelyFetchedValue() == null) {
               returnValue = converter.unboxValue(entry.getValue());
            }
            // Create a copy of the entry to avoid modifying the internal entry
            return entryFactory.create(
                  entry.getKey(), returnValue, entry.getMetadata(),
                  entry.getLifespan(), entry.getMaxIdle());
         } else {
            if (command.getRemotelyFetchedValue() == null) {
               return converter.unboxValue(ret);
            }
            return ret;
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
   public EntryIterable visitEntryRetrievalCommand(InvocationContext ctx, EntryRetrievalCommand command) throws Throwable {
      TypeConverter<Object, Object, Object, Object> converter =
            determineTypeConverter(command.getFlags());
      EntryIterable realIterable = (EntryIterable) super.visitEntryRetrievalCommand(ctx, command);
      return new TypeConverterEntryIterable(realIterable, converter, entryFactory);
   }

   private static class TypeConverterCloseableIterable<K, V> implements CloseableIterable<CacheEntry<K, V>> {
      protected final CloseableIterable iterable;
      protected final TypeConverter<Object, Object, Object, Object> converter;
      protected final InternalEntryFactory entryFactory;

      private TypeConverterCloseableIterable(CloseableIterable iterable,
                                             TypeConverter<Object, Object, Object, Object> converter,
                                             InternalEntryFactory entryFactory) {
         this.iterable = iterable;
         this.converter = converter;
         this.entryFactory = entryFactory;
      }

      @Override
      public void close() {
         iterable.close();
      }

      @Override
      public CloseableIterator<CacheEntry<K, V>> iterator() {
         return new TypeConverterIterator(iterable.iterator(), converter, entryFactory);
      }
   }

   private static class TypeConverterEntryIterable<K, V> extends TypeConverterCloseableIterable<K, V> implements EntryIterable<K, V> {
      private final EntryIterable entryIterable;

      private TypeConverterEntryIterable(EntryIterable iterable, TypeConverter<Object, Object, Object, Object> converter,
                                         InternalEntryFactory entryFactory) {
         super(iterable, converter, entryFactory);
         this.entryIterable = iterable;
      }


      @Override
      public CloseableIterable<CacheEntry> converter(Converter converter) {
         return new TypeConverterCloseableIterable(entryIterable.converter(converter), this.converter, entryFactory);
      }
   }

   private static class TypeConverterIterator<K, V> implements CloseableIterator<CacheEntry<K, V>> {
      private final CloseableIterator<CacheEntry> iterator;
      private final TypeConverter<Object, Object, Object, Object> converter;
      private final InternalEntryFactory entryFactory;

      private TypeConverterIterator(CloseableIterator<CacheEntry> iterator,
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
      public CacheEntry next() {
         CacheEntry entry = iterator.next();
         Object newKey = converter.unboxKey(entry.getKey());
         Object newValue = converter.unboxValue(entry.getValue());
         if (newKey != entry.getKey()) {
            return entryFactory.create(newKey, newValue, entry.getMetadata());
         } else {
            entry.setValue(newValue);
         }
         return entry;
      }

      @Override
      public void remove() {
         iterator.remove();
      }
   }
}
