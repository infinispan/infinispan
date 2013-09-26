package org.infinispan.commands.read;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

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

   public KeySetCommand(DataContainer container, Set<Flag> flags) {
      setFlags(flags);
      this.container = container;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitKeySetCommand(ctx, this);
   }

   @Override
   public Set<Object> perform(InvocationContext ctx) throws Throwable {
      Set<Object> objects = container.keySet();
      if (ctx.getLookedUpEntries().isEmpty()) {
         return new ExpiredFilteredKeySet(objects, container);
      }

      return new FilteredKeySet(objects, ctx.getLookedUpEntries(), container);
   }

   @Override
   public String toString() {
      return "KeySetCommand{" +
            "set=" + container.size() + " elements" +
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
            if (container.containsKey(e.getKey())) {
               if (e.isRemoved()) {
                  size --;
               }
            } else if (!e.isRemoved()) {
               size ++;
            }
         }
         return Math.max(size, 0);
      }

      @Override
      public boolean contains(Object o) {
         CacheEntry e = lookedUpEntries.get(o);
         if (e != null) {
            return !e.isRemoved();
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
                  if (!e.isRemoved()) {
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
                  if (!lookedUpEntries.containsKey(k)) {
                     next = k;
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
               if (container.containsKey(k)) {
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
