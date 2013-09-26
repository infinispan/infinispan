package org.infinispan.persistence.sifs;

import java.util.concurrent.ConcurrentMap;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.CollectionFactory;

/**
 * Table holding the entry positions in log before these are persisted to the index.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class TemporaryTable {

   private ConcurrentMap<Object, Entry> table;

   public TemporaryTable(int capacity, Equivalence<Object> keyEquivalence) {
      table = CollectionFactory.makeConcurrentMap(capacity, keyEquivalence, AnyEquivalence.getInstance());
   }

   public void set(Object key, int file, int offset) {
      for (;;) {
         Entry entry = table.putIfAbsent(key, new Entry(file, offset));
         if (entry != null) {
            synchronized (entry) {
               if (entry.isRemoved()) {
                  continue;
               }
               entry.update(file, offset);
               return;
            }
         } else {
            return;
         }
      }
   }

   public void setConditionally(Object key, int file, int offset, int prevFile, int prevOffset) {
      for (;;) {
         Entry entry = table.putIfAbsent(key, new Entry(file, offset));
         if (entry != null) {
            synchronized (entry) {
               if (entry.isRemoved()) {
                  continue;
               }
               if (entry.getFile() == prevFile && entry.getOffset() == prevOffset) {
                  entry.update(file, offset);
               }
               return;
            }
         } else {
            return;
         }
      }
   }

   public EntryPosition get(Object key) {
      Entry entry = table.get(key);
      if (entry == null) {
         return null;
      }
      int file, offset;
      synchronized (entry) {
         file = entry.getFile();
         offset = entry.getOffset();
      }
      return new EntryPosition(file, offset);
   }

   public void clear() {
      table.clear();
   }

   public void removeConditionally(Object key, int file, int offset) {
      Entry tempEntry = table.get(key);
      if (tempEntry != null) {
         synchronized (tempEntry) {
            if (tempEntry.getFile() == file && tempEntry.getOffset() == offset) {
               table.remove(key, tempEntry);
               tempEntry.setRemoved(true);
            }
         }
      }
   }

   private static class Entry {
      private int file;
      private int offset;
      private boolean removed = false;

      Entry(int file, int offset) {
         this.file = file;
         this.offset = offset;
      }

      public int getFile() {
         return file;
      }

      public int getOffset() {
         return offset;
      }

      public void update(int currentFile, int currentOffset) {
         this.file = currentFile;
         this.offset = currentOffset;
      }

      public boolean isRemoved() {
         return removed;
      }

      public void setRemoved(boolean removed) {
         this.removed = removed;
      }
   }
}
