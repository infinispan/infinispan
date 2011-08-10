/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.interceptors;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.ValuesCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.Immutables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.infinispan.factories.KnownComponentNames.CACHE_MARSHALLER;
import static org.infinispan.marshall.MarshalledValue.isTypeExcluded;

/**
 * Interceptor that handles the wrapping and unwrapping of cached data using {@link
 * org.infinispan.marshall.MarshalledValue}s. Known "excluded" types are not wrapped/unwrapped, which at this time
 * include {@link String}, Java primitives and their Object wrappers, as well as arrays of excluded types.
 * <p/>
 * The {@link org.infinispan.marshall.MarshalledValue} wrapper handles lazy deserialization from byte array
 * representations.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @see org.infinispan.marshall.MarshalledValue
 * @since 4.0
 */
public class MarshalledValueInterceptor extends CommandInterceptor {
   private StreamingMarshaller marshaller;
   private boolean wrapKeys = true;
   private boolean wrapValues = true;

   @Inject
   protected void injectMarshaller(@ComponentName(CACHE_MARSHALLER) StreamingMarshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Start
   protected void start() {
      wrapKeys = configuration.isStoreKeysAsBinary();
      wrapValues = configuration.isStoreValuesAsBinary();
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
      Set<MarshalledValue> marshalledValues = new HashSet<MarshalledValue>();
      Map map = wrapMap(command.getMap(), marshalledValues, ctx);
      command.setMap(map);
      Object retVal = invokeNextInterceptor(ctx, command);
      return compactAndProcessRetVal(marshalledValues, retVal, ctx);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      MarshalledValue key = null;
      MarshalledValue value = null;
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

      // If origin is remote, set equality preference for raw so that deserialization is avoided
      // Don't do this for local invocations so that unnecessary serialization is avoided.
      boolean isRawComparisonRequired = !ctx.isOriginLocal() && command.getKey() instanceof MarshalledValue;
      if (isRawComparisonRequired)
         ((MarshalledValue) command.getKey()).setEqualityPreferenceForInstance(false);

      try {
         Object retVal = invokeNextInterceptor(ctx, command);
         compact(key);
         compact(value);
         return processRetVal(retVal, ctx);
      } finally {
         // Regardless of what happens with the remote key update, revert to equality for instance
         if (isRawComparisonRequired)
            ((MarshalledValue) command.getKey()).setEqualityPreferenceForInstance(true);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      MarshalledValue value = null;
      if (wrapKeys) {
         if (!isTypeExcluded(command.getKey().getClass())) {
            value = createMarshalledValue(command.getKey(), ctx);
            command.setKey(value);
         }
      }
      Object retVal = invokeNextInterceptor(ctx, command);
      compact(value);
      return processRetVal(retVal, ctx);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, org.infinispan.commands.write.EvictCommand command) throws Throwable {
      MarshalledValue value = null;
      if (wrapKeys) {
         if (!isTypeExcluded(command.getKey().getClass())) {
            value = createMarshalledValue(command.getKey(), ctx);
            command.setKey(value);
         }
      }
      Object retVal = invokeNextInterceptor(ctx, command);
      compact(value);
      return processRetVal(retVal, ctx);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      MarshalledValue mv = null;
      if (wrapKeys) {
         if (!isTypeExcluded(command.getKey().getClass())) {
            mv = createMarshalledValue(command.getKey(), ctx);
            command.setKey(mv);
            compact(mv);
         }
      }
      Object retVal = invokeNextInterceptor(ctx, command);
      compact(mv);
      return processRetVal(retVal, ctx);
   }

   @Override
   @SuppressWarnings("unchecked")
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      Set keys = (Set) invokeNextInterceptor(ctx, command);
      if (wrapKeys) {
         Set copy = new HashSet(keys.size());
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
      Collection values = (Collection) invokeNextInterceptor(ctx, command);
      if (wrapValues) {
         Collection copy = new ArrayList();
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
         InternalCacheEntry newEntry = Immutables.immutableInternalCacheEntry(InternalEntryFactory.create(key, value,
                                                                                                          entry.getCreated(), entry.getLifespan(), entry.getLastUsed(), entry.getMaxIdle()));
         copy.add(newEntry);
      }
      return Immutables.immutableSetWrap(copy);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      MarshalledValue key = null, newValue = null, oldValue = null;
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
      compact(key);
      compact(newValue);
      compact(oldValue);
      return processRetVal(retVal, ctx);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      // If origin is remote, set equality preference for raw so that deserialization is avoided
      // Don't do this for local invocations so that unnecessary serialization is avoided.
      boolean isRemote = !ctx.isOriginLocal();
      if (isRemote)
         forceComparison(false, command);

      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         if (isRemote)
            forceComparison(true, command);
      }
   }

   private void forceComparison(boolean isCompareInstance, InvalidateCommand command) {
      if (wrapKeys) {
         for (Object key : command.getKeys()) {
            if (key instanceof MarshalledValue)
               ((MarshalledValue) key).setEqualityPreferenceForInstance(isCompareInstance);
         }
      }
   }

   private Object compactAndProcessRetVal(Set<MarshalledValue> marshalledValues, Object retVal, InvocationContext ctx) {
      if (trace) log.trace("Compacting MarshalledValues created");
      for (MarshalledValue mv : marshalledValues) compact(mv);
      return processRetVal(retVal, ctx);
   }

   private void compact(MarshalledValue mv) {
      if (mv == null) return;
      mv.compact(false, false);
   }

   private Object processRetVal(Object retVal, InvocationContext ctx) {
      if (retVal instanceof MarshalledValue) {
         if (ctx.isOriginLocal()) {
            if (trace) log.tracef("Return is a marshall value, so extract instance from: %s", retVal);
            retVal = ((MarshalledValue) retVal).get();
         }
      }
      return retVal;
   }

   @SuppressWarnings("unchecked")
   protected Map wrapMap(Map<Object, Object> m, Set<MarshalledValue> marshalledValues, InvocationContext ctx) {
      if (m == null) {
         if (trace) log.trace("Map is nul; returning an empty map.");
         return Collections.emptyMap();
      }
      if (trace) log.tracef("Wrapping map contents of argument %s", m);
      Map copy = new HashMap();
      for (Map.Entry me : m.entrySet()) {
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
      return new MarshalledValue(toWrap, ctx.isOriginLocal(), marshaller);
   }
}
