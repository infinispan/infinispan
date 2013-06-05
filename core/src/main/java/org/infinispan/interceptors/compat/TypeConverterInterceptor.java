package org.infinispan.interceptors.compat;

import org.infinispan.CacheException;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.compat.TypeConverter;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.marshall.Marshaller;
import org.infinispan.metadata.Metadata;

import java.util.ServiceLoader;
import java.util.Set;

/**
 * An interceptor that applies type conversion to the data stored in the cache.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class TypeConverterInterceptor extends CommandInterceptor {

   // No need for a REST type converter since the REST server itself does
   // the hard work of converting from one content type to the other

   private TypeConverter<Object, Object, Object, Object> hotRodConverter;
   private TypeConverter<Object, Object, Object, Object> memcachedConverter;
   private TypeConverter<Object, Object, Object, Object> embeddedConverter;
   private InternalEntryFactory entryFactory;
   private VersionGenerator versionGenerator;

   @SuppressWarnings("unchecked")
   public TypeConverterInterceptor(Marshaller marshaller) {
      ServiceLoader<TypeConverter> converters = ServiceLoader.load(TypeConverter.class);
      for (TypeConverter converter : converters) {
         if (converter.supportsInvocation(Flag.OPERATION_HOTROD)) {
            hotRodConverter = setConverterMarshaller(converter, marshaller);
         } else if (converter.supportsInvocation(Flag.OPERATION_MEMCACHED)) {
            memcachedConverter = setConverterMarshaller(converter, marshaller);
         }
      }
      embeddedConverter = setConverterMarshaller(new EmbeddedTypeConverter(), marshaller);
   }

   private TypeConverter setConverterMarshaller(TypeConverter converter, Marshaller marshaller) {
      if (marshaller != null)
         converter.setMarshaller(marshaller);

      return converter;
   }

   @Inject
   public void init(InternalEntryFactory entryFactory, VersionGenerator versionGenerator) {
      this.entryFactory = entryFactory;
      this.versionGenerator = versionGenerator;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      TypeConverter<Object, Object, Object, Object> converter =
            determineTypeConverter(command.getFlags());
      command.setKey(converter.boxKey(key));
      command.setValue(converter.boxValue(command.getValue()));
      Object ret = invokeNextInterceptor(ctx, command);
      return converter.unboxValue(ret);
   }

   private TypeConverter<Object, Object, Object, Object> determineTypeConverter(Set<Flag> flags) {
      if (flags != null) {
         if (flags.contains(Flag.OPERATION_HOTROD))
            return hotRodConverter;
         else if (flags.contains(Flag.OPERATION_MEMCACHED))
            return memcachedConverter;
      }

      return embeddedConverter;
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      TypeConverter<Object, Object, Object, Object> converter =
            determineTypeConverter(command.getFlags());
      command.setKey(converter.boxKey(key));
      Object ret = invokeNextInterceptor(ctx, command);
      if (ret != null) {
         if (command.isReturnEntry()) {
            InternalCacheEntry entry = (InternalCacheEntry) ret;
            Object returnValue = converter.unboxValue(entry.getValue());
            // Create a copy of the entry to avoid modifying the internal entry
            return entryFactory.create(
                  entry.getKey(), returnValue, entry.getMetadata(),
                  entry.getLifespan(), entry.getMaxIdle());
         }

         return converter.unboxValue(ret);
      }

      return null;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object key = command.getKey();
      TypeConverter<Object, Object, Object, Object> converter =
            determineTypeConverter(command.getFlags());
      command.setKey(converter.boxKey(key));
      Object oldValue = command.getOldValue();
      command.setOldValue(converter.boxValue(oldValue));
      command.setNewValue(converter.boxValue(command.getNewValue()));
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
      command.setKey(converter.boxKey(key));
      Object conditionalValue = command.getValue();
      command.setValue(converter.boxValue(conditionalValue));
      Object ret = invokeNextInterceptor(ctx, command);

      // Return of conditional remove is not the value type, but boolean, so
      // apply an exception that applies to all servers, regardless of what's
      // stored in the value side
      if (conditionalValue != null && ret instanceof Boolean)
         return ret;

      return converter.unboxValue(ret);
   }

   private static class EmbeddedTypeConverter
         implements TypeConverter<Object, Object, Object, Object> {

      private Marshaller marshaller;

      @Override
      public Object boxKey(Object key) {
         return key;
      }

      @Override
      public Object boxValue(Object value) {
         return value;
      }

      @Override
      public Object unboxValue(Object target) {
         if (marshaller != null && target instanceof byte[]) {
            try {
               return marshaller.objectFromByteBuffer((byte[]) target);
            } catch (Exception e) {
               throw new CacheException("Unable to unmarshall return value");
            }
         }

         return target;
      }

      @Override
      public boolean supportsInvocation(Flag flag) {
         return false;
      }

      @Override
      public void setMarshaller(Marshaller marshaller) {
         this.marshaller = marshaller;
      }

   }

}
