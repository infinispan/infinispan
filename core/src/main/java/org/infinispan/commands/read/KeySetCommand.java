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

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;

/**
 * Command implementation for {@link java.util.Map#keySet()} functionality.
 *
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @since 4.0
 */
public class KeySetCommand extends AbstractLocalCommand implements VisitableCommand {
   private final DataContainer container;

   public KeySetCommand(DataContainer container) {
      this.container = container;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitKeySetCommand(ctx, this);
   }

   @Override
   public Set<Object> perform(InvocationContext ctx) throws Throwable {
      Set<Object> objects = container.keySet();
      if (noTxModifications(ctx)) {
         return new ExpiredFilteredKeySet(objects, container);
      }

      return new FilteredKeySet(objects, ctx.getLookedUpEntries(), container);
   }

   @Override
   public String toString() {
      return "KeySetCommand{" +
            "set=" + container.keySet() +
            '}';
   }

   private static class FilteredKeySet extends AbstractSet<Object> {
      final Set<Object> keySet;
      final Map<Object, CacheEntry> lookedUpEntries;
      final DataContainer container;

      FilteredKeySet(Set<Object> keySet, Map<Object, CacheEntry> lookedUpEntries, DataContainer container) {
         this.keySet = keySet;
         this.lookedUpEntries = lookedUpEntries;
         this.container = container;
      }

      @Override
      public int size() {
         int size = keySet.size();
         // First, removed any expired keys
         for (Object k : keySet) {
            // Given the key set, a key won't be contained if it's expired
            if (!container.containsKey(k))
               size--;
         }
         // Update according to keys added or removed in tx
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
         CacheEntry e = lookedUpEntries.get(o);
         if (e == null || e.isRemoved()) {
            return false;
         } else if (e.isChanged() || e.isCreated()) {
            return true;
         }
         return keySet.contains(o);
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
      public boolean addAll(Collection<? extends Object> c) {
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
         private final Iterator<Object> it2 = keySet.iterator();
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
                     next = e.getKey();
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
               while (it2.hasNext()) {
                  Object k = it2.next();
                  CacheEntry e = lookedUpEntries.get(k);
                  if (e == null || !e.isRemoved()) {
                     next = k;
                     found = true;
                     break;
                  }

                  // Skip keys that would have been expired
                  if (!container.containsKey(k)) {
                     continue;
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

   public static class ExpiredFilteredKeySet extends AbstractSet<Object> {
      final Set<Object> keySet;
      final DataContainer container;

      public ExpiredFilteredKeySet(Set<Object> keySet, DataContainer container) {
         this.keySet = keySet;
         this.container = container;
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
      public boolean addAll(Collection<? extends Object> c) {
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
      public Iterator<Object> iterator() {
         return new Itr();
      }

      @Override
      public int size() {
         // Size cannot be cached because even if the set is immutable,
         // over time, the expired entries could grow hence reducing the size
         int s = keySet.size();
         for (Object k : keySet) {
            // Given the key set, a key won't be contained if it's expired
            if (!container.containsKey(k))
               s--;
         }
         return s;
      }

      private class Itr implements Iterator<Object> {

         private final Iterator<Object> it = keySet.iterator();
         private Object next;

         private Itr() {
            fetchNext();
         }

         private void fetchNext() {
            while (it.hasNext()) {
               Object k = it.next();
               if (!container.containsKey(k)) {
                  continue;
               } else {
                  next = k;
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
