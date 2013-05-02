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
package org.infinispan.commands.read;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.TimeService;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Command implementation for {@link java.util.Map#values()} functionality.
 *
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @since 4.0
 */
public class ValuesCommand extends AbstractLocalCommand implements VisitableCommand {
   private final DataContainer container;
   private final TimeService timeService;

   public ValuesCommand(DataContainer container, TimeService timeService) {
      this.container = container;
      this.timeService = timeService;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitValuesCommand(ctx, this);
   }

   @Override
   public Collection<Object> perform(InvocationContext ctx) throws Throwable {
      if (noTxModifications(ctx)) {
         return new ExpiredFilteredValues(container.entrySet(), timeService);
      }

      return new FilteredValues(container, ctx.getLookedUpEntries(), timeService);
   }

   @Override
   public String toString() {
      return "ValuesCommand{" +
            "values=" + container.size() + " elements" +
            '}';
   }

   private static class FilteredValues extends AbstractCollection<Object> {
      final Collection<Object> values;
      final Set<InternalCacheEntry> entrySet;
      final Map<Object, CacheEntry> lookedUpEntries;
      final TimeService timeService;

      FilteredValues(DataContainer container, Map<Object, CacheEntry> lookedUpEntries, TimeService timeService) {
         values = container.values();
         entrySet = container.entrySet();
         this.lookedUpEntries = lookedUpEntries;
         this.timeService = timeService;
      }

      @Override
      public int size() {
         long currentTimeMillis = 0;
         int size = entrySet.size();
         // First, removed any expired ones
         for (InternalCacheEntry e: entrySet) {
            if (e.canExpire()) {
               if (currentTimeMillis == 0)
                  currentTimeMillis = timeService.wallClockTime();
               if (e.isExpired(currentTimeMillis))
                  size--;
            }
         }
         // Update according to entries added or removed in tx
         for (CacheEntry e: lookedUpEntries.values()) {
            if (e.isCreated()) {
               size ++;
            } else if (e.isRemoved()) {
               size --;
            }
         }
         return Math.max(size, 0);
      }

      @Override
      public boolean contains(Object o) {
         for (CacheEntry e: lookedUpEntries.values()) {
            if (o.equals(e.getValue())) {
               return !e.isRemoved();
            }
         }

         return values.contains(o);
      }

      @Override
      public Iterator<Object> iterator() {
         return new Itr();
      }

      @Override
      public boolean add(Object e) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean remove(Object o) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean addAll(Collection<?> c) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean retainAll(Collection<?> c) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean removeAll(Collection<?> c) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
         throw new UnsupportedOperationException();
      }

      private class Itr implements Iterator<Object> {

         private final Iterator<CacheEntry> it1 = lookedUpEntries.values().iterator();
         private final Iterator<InternalCacheEntry> it2 = entrySet.iterator();
         private boolean atIt1 = true;
         private Object next;

         Itr() {
            fetchNext();
         }

         private void fetchNext() {
            if (atIt1) {
               boolean found = false;
               while (it1.hasNext()) {
                  CacheEntry e = it1.next();
                  if (e.isCreated()) {
                     next = e.getValue();
                     found = true;
                     break;
                  }
               }

               if (!found) {
                  atIt1 = false;
               }
            }

            if (!atIt1) {
               boolean found = false;
               long currentTimeMillis = 0;
               while (it2.hasNext()) {
                  InternalCacheEntry ice = it2.next();
                  Object key = ice.getKey();
                  CacheEntry e = lookedUpEntries.get(key);
                  if (ice.canExpire()) {
                     if (currentTimeMillis == 0)
                        currentTimeMillis = timeService.wallClockTime();
                     if (ice.isExpired(currentTimeMillis))
                        continue;
                  }

                  if (e == null) {
                     next = ice.getValue();
                     found = true;
                     break;
                  }
                  if (e.isChanged()) {
                     next = e.getValue();
                     found = true;
                     break;
                  }
               }

               if (!found) {
                  next = null;
               }
            }
         }

         @Override
         public boolean hasNext() {
            if (next == null) {
               fetchNext();
            }
            return next != null;
         }

         @Override
         public Object next() {
            if (next == null) {
               fetchNext();
            }

            if (next == null) {
               throw new NoSuchElementException();
            }

            Object ret = next;
            next = null;
            return ret;
         }

         @Override
         public void remove() {
            throw new UnsupportedOperationException();
         }
      }
   }

   public static class ExpiredFilteredValues extends AbstractCollection<Object> {
      final Set<InternalCacheEntry> entrySet;
      final TimeService timeService;

      public ExpiredFilteredValues(Set<InternalCacheEntry> entrySet, TimeService timeService) {
         this.entrySet = entrySet;
         this.timeService = timeService;
      }

      @Override
      public Iterator<Object> iterator() {
         return new Itr();
      }

      @Override
      public boolean add(Object e) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean remove(Object o) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean addAll(Collection<?> c) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean retainAll(Collection<?> c) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean removeAll(Collection<?> c) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
         throw new UnsupportedOperationException();
      }

      @Override
      public int size() {
         // Use entry set as a way to calculate the number of values.
         int s = entrySet.size();
         long currentTimeMillis = 0;
         for (InternalCacheEntry e: entrySet) {
            if (e.canExpire()) {
               if (currentTimeMillis==0)
                  currentTimeMillis = timeService.wallClockTime();
               if (e.isExpired(currentTimeMillis))
                  s--;
            }
         }
         return s;
      }

      private class Itr implements Iterator<Object> {

         private final Iterator<InternalCacheEntry> it = entrySet.iterator();
         private Object next;

         private Itr() {
            fetchNext();
         }

         private void fetchNext() {
            long currentTimeMillis = 0;
            while (it.hasNext()) {
               InternalCacheEntry e = it.next();
               final boolean canExpire = e.canExpire();
               if (canExpire && currentTimeMillis == 0) {
                  currentTimeMillis = timeService.wallClockTime();
               }
               if (!canExpire) {
                  next = e.getValue();
                  break;
               } else if (!e.isExpired(currentTimeMillis)) {
                  next = e.getValue();
                  break;
               }
            }
         }

         @Override
         public boolean hasNext() {
            if (next == null)
               fetchNext();

            return next != null;
         }

         @Override
         public Object next() {
            if (next == null)
               fetchNext();

            if (next == null)
               throw new NoSuchElementException();

            Object ret = next;
            next = null;
            return ret;
         }

         @Override
         public void remove() {
            throw new UnsupportedOperationException();
         }
      }
   }

}
