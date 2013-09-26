package org.infinispan.commands.read;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.DataContainer;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.TimeService;
import static org.infinispan.util.CoreImmutables.immutableInternalCacheEntry;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Command implementation for {@link java.util.Map#entrySet()} functionality.
 *
 * @author Galder Zamarreño
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @since 4.0
 */
public class EntrySetCommand extends AbstractLocalCommand implements VisitableCommand {
   private final DataContainer container;
   private final InternalEntryFactory entryFactory;
   private final TimeService timeService;

   public EntrySetCommand(DataContainer container, InternalEntryFactory internalEntryFactory, TimeService timeService, Set<Flag> flags) {
      setFlags(flags);
      this.container = container;
      this.entryFactory = internalEntryFactory;
      this.timeService = timeService;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitEntrySetCommand(ctx, this);
   }

   @Override
   public Set<InternalCacheEntry> perform(InvocationContext ctx) throws Throwable {
      Set<InternalCacheEntry> entries = container.entrySet();
      return createFilteredEntrySet(entries, ctx, timeService, entryFactory);
   }

   public static Set<InternalCacheEntry> createFilteredEntrySet(
         Set<InternalCacheEntry> entries, InvocationContext ctx,
         TimeService timeService, InternalEntryFactory entryFactory) {
      if (ctx.getLookedUpEntries().isEmpty()) {
         return new ExpiredFilteredEntrySet(entries, timeService);
      }

      return new FilteredEntrySet(entries, ctx.getLookedUpEntries(), timeService, entryFactory);
   }

   @Override
   public String toString() {
      return "EntrySetCommand{" +
            "set=" + container.size() + " elements" +
            '}';
   }

   private static class FilteredEntrySet extends AbstractSet<InternalCacheEntry> {
      final Set<InternalCacheEntry> entrySet;
      final Map<Object, CacheEntry> lookedUpEntries;
      final TimeService timeService;
      final InternalEntryFactory entryFactory;

      FilteredEntrySet(Set<InternalCacheEntry> entrySet, Map<Object, CacheEntry> lookedUpEntries,
            TimeService timeService, InternalEntryFactory entryFactory) {
         this.entrySet = entrySet;
         this.lookedUpEntries = lookedUpEntries;
         this.timeService = timeService;
         this.entryFactory = entryFactory;
      }

      @Override
      public int size() {
         long currentTimeMillis = 0;
         Set<Object> validKeys = new HashSet<Object>();
         // First, removed any expired ones
         for (InternalCacheEntry e: entrySet) {
            if (e.canExpire()) {
               if (currentTimeMillis == 0)
                  currentTimeMillis = timeService.wallClockTime();
               if (!e.isExpired(currentTimeMillis))
                  validKeys.add(e.getKey());
            }
            else {
               validKeys.add(e.getKey());
            }
         }

         int size = validKeys.size();
         // Update according to entries added or removed in tx
         for (CacheEntry e: lookedUpEntries.values()) {
            if (validKeys.contains(e.getKey())) {
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
         if (!(o instanceof Map.Entry)) {
            return false;
         }

         @SuppressWarnings("rawtypes")
         Map.Entry e = (Map.Entry) o;
         CacheEntry ce = lookedUpEntries.get(e.getKey());
         if (ce != null) {
            return !ce.isRemoved();
         }

         return entrySet.contains(o);
      }

      @Override
      public Iterator<InternalCacheEntry> iterator() {
         return new Itr();
      }

      @Override
      public boolean add(InternalCacheEntry e) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean remove(Object o) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean addAll(Collection<? extends InternalCacheEntry> c) {
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

      private class Itr implements Iterator<InternalCacheEntry> {

         private final Iterator<CacheEntry> it1 = lookedUpEntries.values().iterator();
         private final Iterator<InternalCacheEntry> it2 = entrySet.iterator();
         private boolean atIt1 = true;
         private InternalCacheEntry next;

         Itr() {
            fetchNext();
         }

         private void fetchNext() {
            if (atIt1) {
               boolean found = false;
               while (it1.hasNext()) {
                  CacheEntry e = it1.next();
                  if (!e.isRemoved()) {
                     next = immutableInternalCacheEntry(entryFactory.create(e.getKey(), e.getValue(), e.getMetadata()));
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
                  InternalCacheEntry ice = it2.next();
                  Object key = ice.getKey();
                  if (!lookedUpEntries.containsKey(key)) {
                     next = ice;
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
         public InternalCacheEntry next() {
            if (next == null) {
               fetchNext();
            }

            if (next == null) {
               throw new NoSuchElementException();
            }

            InternalCacheEntry ret = next;
            next = null;
            return ret;
         }

         @Override
         public void remove() {
            throw new UnsupportedOperationException();
         }
      }
   }

   /**
    * An immutable entry set that filters out expired entries. The reason for
    * making it immutable is to avoid having to do further wrapping with an
    * immutable delegate.
    */
   private static class ExpiredFilteredEntrySet extends AbstractSet<InternalCacheEntry> {

      final Set<InternalCacheEntry> entrySet;
      final TimeService timeService;

      public ExpiredFilteredEntrySet(Set<InternalCacheEntry> entrySet, TimeService timeService) {
         this.entrySet = entrySet;
         this.timeService = timeService;
      }

      @Override
      public Iterator<InternalCacheEntry> iterator() {
         return new Itr();
      }

      @Override
      public int size() {
         // Size cannot be cached because even if the set is immutable,
         // over time, the expired entries could grow hence reducing the size
         int s = entrySet.size();
         long currentTimeMillis = 0;
         for (InternalCacheEntry e: entrySet) {
            if (e.canExpire()) {
               if (currentTimeMillis == 0)
                  currentTimeMillis = timeService.wallClockTime();
               if (e.isExpired(currentTimeMillis))
                  s--;
            }
         }
         return s;
      }

      @Override
      public boolean add(InternalCacheEntry e) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean remove(Object o) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean addAll(Collection<? extends InternalCacheEntry> c) {
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

      private class Itr implements Iterator<InternalCacheEntry> {

         private final Iterator<InternalCacheEntry> it = entrySet.iterator();
         private InternalCacheEntry next;

         private Itr() {
            fetchNext();
         }

         private void fetchNext() {
            long currentTimeMillis = -1; //lazily look at the wall clock: we want to look no more than once, but not at all if all entries are immortal.
            while (it.hasNext()) {
               InternalCacheEntry e = it.next();
               if (e.canExpire()) {
                  if (currentTimeMillis == -1) currentTimeMillis = timeService.wallClockTime();
                  if (! e.isExpired(currentTimeMillis)) {
                     next = e;
                     return;
                  }
               }
               else {
                  next = e;
                  return;
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
         public InternalCacheEntry next() {
            if (next == null)
               fetchNext();

            if (next == null)
               throw new NoSuchElementException();

            InternalCacheEntry ret = next;
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
