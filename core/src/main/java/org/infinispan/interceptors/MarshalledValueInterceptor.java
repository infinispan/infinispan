package org.infinispan.interceptors;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.ValuesCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.util.CoreImmutables;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.infinispan.factories.KnownComponentNames.CACHE_MARSHALLER;
import static org.infinispan.marshall.core.MarshalledValue.isTypeExcluded;

/**
 * Interceptor that handles the wrapping and unwrapping of cached data using {@link
 * org.infinispan.marshall.core.MarshalledValue}s. Known "excluded" types are not wrapped/unwrapped, which at this time
 * include {@link String}, Java primitives and their Object wrappers, as well as arrays of excluded types.
 * <p/>
 * The {@link org.infinispan.marshall.core.MarshalledValue} wrapper handles lazy deserialization from byte array
 * representations.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @see org.infinispan.marshall.core.MarshalledValue
 * @since 4.0
 */
public class MarshalledValueInterceptor extends CommandInterceptor {
   private StreamingMarshaller marshaller;
   private boolean wrapKeys = true;
   private boolean wrapValues = true;
   private InternalEntryFactory entryFactory;

   private static final Log log = LogFactory.getLog(MarshalledValueInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   protected void injectMarshaller(@ComponentName(CACHE_MARSHALLER) StreamingMarshaller marshaller, InternalEntryFactory entryFactory) {
      this.marshaller = marshaller;
      this.entryFactory = entryFactory;
   }

   @Start
   protected void start() {
      wrapKeys = cacheConfiguration.storeAsBinary().storeKeysAsBinary();
      wrapValues = cacheConfiguration.storeAsBinary().storeValuesAsBinary();
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
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

      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Set<MarshalledValue> marshalledValues = new HashSet<MarshalledValue>(command.getMap().size());
      Map<Object, Object> map = wrapMap(command.getMap(), marshalledValues, ctx);
      command.setMap(map);
      Object retVal = invokeNextInterceptor(ctx, command);
      return processRetVal(retVal, ctx);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
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

      Object retVal = invokeNextInterceptor(ctx, command);
      return processRetVal(retVal, ctx);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      MarshalledValue value;
      if (wrapKeys) {
         if (!isTypeExcluded(command.getKey().getClass())) {
            value = createMarshalledValue(command.getKey(), ctx);
            command.setKey(value);
         }
      }
      Object retVal = invokeNextInterceptor(ctx, command);
      return processRetVal(retVal, ctx);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, org.infinispan.commands.write.EvictCommand command) throws Throwable {
      MarshalledValue value;
      if (wrapKeys) {
         if (!isTypeExcluded(command.getKey().getClass())) {
            value = createMarshalledValue(command.getKey(), ctx);
            command.setKey(value);
         }
      }
      Object retVal = invokeNextInterceptor(ctx, command);
      return processRetVal(retVal, ctx);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      MarshalledValue mv;
      if (wrapKeys) {
         if (!isTypeExcluded(command.getKey().getClass())) {
            mv = createMarshalledValue(command.getKey(), ctx);
            command.setKey(mv);
         }
      }
      Object retVal = invokeNextInterceptor(ctx, command);
      return processRetVal(retVal, ctx);
   }

   @Override
   @SuppressWarnings("unchecked")
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      Set<Object> keys = (Set<Object>) invokeNextInterceptor(ctx, command);
      if (wrapKeys) {
         Set<Object> copy = new HashSet<Object>(keys.size());
         for (Object key : keys) {
            if (key instanceof MarshalledValue) {
               key = ((MarshalledValue) key).get();
            }
            copy.add(key);
         }
         return Immutables.immutableSetWrap(copy);
      } else {
         return Immutables.immutableSetWrap(keys);
      }
   }

   @Override
   @SuppressWarnings("unchecked")
   public Object visitValuesCommand(InvocationContext ctx, ValuesCommand command) throws Throwable {
      Collection<Object> values = (Collection<Object>) invokeNextInterceptor(ctx, command);
      if (wrapValues) {
         Collection<Object> copy = new ArrayList<Object>();
         for (Object value : values) {
            if (value instanceof MarshalledValue) {
               value = ((MarshalledValue) value).get();
            }
            copy.add(value);
         }
         return Immutables.immutableCollectionWrap(copy);
      } else {
         return Immutables.immutableCollectionWrap(values);
      }
   }

   @Override
   @SuppressWarnings("unchecked")
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      Set<InternalCacheEntry> entries = (Set<InternalCacheEntry>) invokeNextInterceptor(ctx, command);
      Set<InternalCacheEntry> copy = new HashSet<InternalCacheEntry>(entries.size());
      for (InternalCacheEntry entry : entries) {
         Object key = entry.getKey();
         Object value = entry.getValue();
         if (key instanceof MarshalledValue) {
            key = ((MarshalledValue) key).get();
         }
         if (value instanceof MarshalledValue) {
            value = ((MarshalledValue) value).get();
         }
         InternalCacheEntry newEntry = CoreImmutables.immutableInternalCacheEntry(entryFactory.create(key, value, entry));
         copy.add(newEntry);
      }
      return Immutables.immutableSetWrap(copy);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
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
      Object retVal = invokeNextInterceptor(ctx, command);
      return processRetVal(retVal, ctx);
   }

   protected Object processRetVal(Object retVal, InvocationContext ctx) {
      if (retVal instanceof MarshalledValue) {
         if (ctx.isOriginLocal()) {
            if (trace) log.tracef("Return is a marshall value, so extract instance from: %s", retVal);
            retVal = ((MarshalledValue) retVal).get();
         }
      }
      return retVal;
   }

   @SuppressWarnings("unchecked")
   protected Map<Object, Object> wrapMap(Map<Object, Object> m, Set<MarshalledValue> marshalledValues, InvocationContext ctx) {
      if (m == null) {
         if (trace) log.trace("Map is nul; returning an empty map.");
         return InfinispanCollections.emptyMap();
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
