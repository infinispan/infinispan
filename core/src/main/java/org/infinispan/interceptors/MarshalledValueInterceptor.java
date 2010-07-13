/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.ValuesCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.Immutables;

import java.io.IOException;
import java.io.NotSerializableException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Interceptor that handles the wrapping and unwrapping of cached data using {@link
 * org.infinispan.marshall.MarshalledValue}s. Known "excluded" types are not wrapped/unwrapped, which at this time include
 * {@link String}, Java primitives and their Object wrappers, as well as arrays of excluded types.
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
   
   @Inject
   protected void injectMarshaller(StreamingMarshaller marshaller) {
      this.marshaller = marshaller;
   }
   
   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Set<MarshalledValue> marshalledValues = new HashSet<MarshalledValue>();
      Map map = wrapMap(command.getMap(), marshalledValues, ctx);
      command.setMap(map);
      Object retVal = invokeNextInterceptor(ctx, command);
      return compactAndProcessRetVal(marshalledValues, retVal);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      MarshalledValue key = null;
      MarshalledValue value = null;
      if (!MarshalledValue.isTypeExcluded(command.getKey().getClass())) {
         key = createMarshalledValue(command.getKey(), ctx);
         command.setKey(key);
      }
      if (!MarshalledValue.isTypeExcluded(command.getValue().getClass())) {
         value = createMarshalledValue(command.getValue(), ctx);
         command.setValue(value);
      }
      Object retVal = invokeNextInterceptor(ctx, command);
      compact(key);
      compact(value);
      return processRetVal(retVal);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      MarshalledValue value = null;
      if (!MarshalledValue.isTypeExcluded(command.getKey().getClass())) {
         value = createMarshalledValue(command.getKey(), ctx);
         command.setKey(value);
      }
      Object retVal = invokeNextInterceptor(ctx, command);
      compact(value);
      return processRetVal(retVal);
   }
   
   @Override
   public Object visitEvictCommand(InvocationContext ctx, org.infinispan.commands.write.EvictCommand command) throws Throwable {
      MarshalledValue value = null;
      if (!MarshalledValue.isTypeExcluded(command.getKey().getClass())) {
         value = createMarshalledValue(command.getKey(), ctx);
         command.setKey(value);
      }
      Object retVal = invokeNextInterceptor(ctx, command);
      compact(value);
      return processRetVal(retVal);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      MarshalledValue mv = null;
      if (!MarshalledValue.isTypeExcluded(command.getKey().getClass())) {
         mv = createMarshalledValue(command.getKey(), ctx);
         command.setKey(mv);
         compact(mv);
      }
      Object retVal = invokeNextInterceptor(ctx, command);
      compact(mv);
      return processRetVal(retVal);
   }
   
   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      Set keys = (Set) invokeNextInterceptor(ctx, command);
      Set copy = new HashSet(keys.size());
      for (Object key : keys) {
         if (key instanceof MarshalledValue) {
            key = ((MarshalledValue) key).get();
         } 
         copy.add(key);
      }
      return Immutables.immutableSetWrap(copy);
   }

   @Override
   public Object visitValuesCommand(InvocationContext ctx, ValuesCommand command) throws Throwable {
      Collection values = (Collection) invokeNextInterceptor(ctx, command);
      Collection copy = new ArrayList();  
      for (Object value : values) {
         if (value instanceof MarshalledValue) {
            value = ((MarshalledValue) value).get();
         }
         copy.add(value);
      }
      return Immutables.immutableCollectionWrap(copy);
   }
   
   @Override
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

   private Object compactAndProcessRetVal(Set<MarshalledValue> marshalledValues, Object retVal)
         throws IOException, ClassNotFoundException {
      if (trace) log.trace("Compacting MarshalledValues created");
      for (MarshalledValue mv : marshalledValues) compact(mv);
      return processRetVal(retVal);
   }

   private void compact(MarshalledValue mv) {
      if (mv == null) return;
      mv.compact(false, false);
   }

   private Object processRetVal(Object retVal) throws IOException, ClassNotFoundException {
      if (retVal instanceof MarshalledValue) {
         if (trace) log.trace("Return is a marshall value, so extract instance from: {0}", retVal);
         retVal = ((MarshalledValue) retVal).get();
      }
      return retVal;
   }

   @SuppressWarnings("unchecked")
   protected Map wrapMap(Map<Object, Object> m, Set<MarshalledValue> marshalledValues, InvocationContext ctx) throws NotSerializableException {
      if (m == null) {
         if (trace) log.trace("Map is nul; returning an empty map.");
         return Collections.emptyMap();
      }
      if (trace) log.trace("Wrapping map contents of argument " + m);
      Map copy = new HashMap();
      for (Map.Entry me : m.entrySet()) {
         Object key = me.getKey();
         Object value = me.getValue();
         Object newKey = (key == null || MarshalledValue.isTypeExcluded(key.getClass())) ? key : createMarshalledValue(key, ctx);
         Object newValue = (value == null || MarshalledValue.isTypeExcluded(value.getClass())) ? value : createMarshalledValue(value, ctx);
         if (newKey instanceof MarshalledValue) marshalledValues.add((MarshalledValue) newKey);
         if (newValue instanceof MarshalledValue) marshalledValues.add((MarshalledValue) newValue);
         copy.put(newKey, newValue);
      }
      return copy;
   }

   protected MarshalledValue createMarshalledValue(Object toWrap, InvocationContext ctx) throws NotSerializableException {
      return new MarshalledValue(toWrap, ctx.isOriginLocal(), marshaller);
   }
}
