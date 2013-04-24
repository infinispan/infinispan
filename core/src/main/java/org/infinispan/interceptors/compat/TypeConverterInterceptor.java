package org.infinispan.interceptors.compat;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.VersionedPutKeyValueCommand;
import org.infinispan.compat.MarshalledTypeConverter;
import org.infinispan.compat.TypeConverter;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public class TypeConverterInterceptor extends CommandInterceptor {

   private final TypeConverter<Object, Object, Object, Object> typeConverter;
   private InternalEntryFactory entryFactory;

   @SuppressWarnings("unchecked")
   public TypeConverterInterceptor(TypeConverter typeConverter) {
      this.typeConverter = typeConverter;
   }

   @Inject
   public void init(InternalEntryFactory entryFactory) {
      this.entryFactory = entryFactory;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      command.setKey(typeConverter.boxKey(key));
      command.setValue(typeConverter.boxValue(key, command.getValue()));
      Object ret = invokeNextInterceptor(ctx, command);
      return typeConverter.unboxValue(key, ret);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      command.setKey(typeConverter.boxKey(key));
      Object ret = super.visitGetKeyValueCommand(ctx, command);
      return typeConverter.unboxValue(key, ret);
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      command.setKey(typeConverter.boxKey(key));
      Object ret = invokeNextInterceptor(ctx, command);
      if (ret != null) {
         InternalCacheEntry entry = (InternalCacheEntry) ret;
         Object returnValue = typeConverter.unboxValue(key, entry.getValue());
         // Create a copy of the entry to avoid modifying the internal entry
         return entryFactory.create(
               entry.getKey(), returnValue, entry.getVersion(),
               entry.getLifespan(), entry.getMaxIdle());
      }

      return null;
   }


   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object key = command.getKey();
      command.setKey(typeConverter.boxKey(key));
      Object oldValue = command.getOldValue();
      command.setOldValue(typeConverter.boxValue(key, oldValue));
      command.setNewValue(typeConverter.boxValue(key, command.getNewValue()));
      Object ret = invokeNextInterceptor(ctx, command);

      // Return of conditional replace is not the value type, but boolean, so
      // apply an exception that applies to all servers, regardless of what's
      // stored in the value side
      if (oldValue != null && ret instanceof Boolean)
         return ret;

      return typeConverter.unboxValue(key, ret);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object key = command.getKey();
      command.setKey(typeConverter.boxKey(key));
      Object conditionalValue = command.getValue();
      command.setValue(typeConverter.boxValue(key, conditionalValue));
      Object ret = invokeNextInterceptor(ctx, command);

      // Return of conditional remove is not the value type, but boolean, so
      // apply an exception that applies to all servers, regardless of what's
      // stored in the value side
      if (conditionalValue != null && ret instanceof Boolean)
         return ret;

      return typeConverter.unboxValue(key, ret);
   }

   // TODO: getWithMetadata vs getCacheEntry...

}
