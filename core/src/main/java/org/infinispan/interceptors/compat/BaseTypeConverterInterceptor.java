package org.infinispan.interceptors.compat;

import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.Immutables;
import org.infinispan.compat.TypeConverter;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.TimeService;

import java.util.HashSet;
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
   private TimeService timerService;

   @Inject
   protected void init(InternalEntryFactory entryFactory, VersionGenerator versionGenerator, TimeService timeService) {
      this.entryFactory = entryFactory;
      this.versionGenerator = versionGenerator;
      this.timerService = timeService;
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
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      Set<CacheEntry> set = (Set<CacheEntry>) super.visitEntrySetCommand(ctx, command);
      TypeConverter<Object, Object, Object, Object> converter =
            determineTypeConverter(command.getFlags());
      Set<InternalCacheEntry> backingSet = new HashSet<InternalCacheEntry>(set.size());
      for (CacheEntry entry : set) {
         Object key = converter.unboxKey(entry.getKey());
         Object value = converter.unboxValue(entry.getValue());
         InternalCacheEntry newEntry = entryFactory.create(
               key, value, entry.getMetadata(),
               entry.getLifespan(), entry.getMaxIdle());
         backingSet.add(newEntry);
      }

      return EntrySetCommand.createFilteredEntrySet(backingSet, ctx, timerService, entryFactory);
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      Set<Object> keySet = (Set<Object>) super.visitKeySetCommand(ctx, command);
      TypeConverter<Object, Object, Object, Object> converter =
            determineTypeConverter(command.getFlags());

      Set<Object> backingSet = new HashSet<Object>(keySet.size());
      for (Object key : keySet)
         backingSet.add(converter.unboxKey(key));

      // Returning a filtered key set here is difficult because it uses the
      // container as a way to find out if the key is expired or not. Since
      // the keys are unboxed here, no keys will be found in the data container.
      return Immutables.immutableSetWrap(backingSet);
   }
}
