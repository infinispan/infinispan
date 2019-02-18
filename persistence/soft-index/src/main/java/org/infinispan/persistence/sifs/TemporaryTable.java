package org.infinispan.persistence.sifs;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.util.logging.LogFactory;

/**
 * Table holding the entry positions in log before these are persisted to the index.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class TemporaryTable {
   private static final Log log = LogFactory.getLog(TemporaryTable.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private ConcurrentMap<Object, Entry> table;

   public TemporaryTable(int capacity) {
      table = new ConcurrentHashMap<>(capacity);
   }

   public void set(Object key, int file, int offset) {
      for (; ; ) {
         Entry entry = table.putIfAbsent(key, new Entry(file, offset, false));
         if (entry != null) {
            synchronized (entry) {
               if (entry.isRemoved()) {
                  continue;
               } else if (entry.isLocked()) {
                  try {
                     if (trace) {
                        log.tracef("Waiting for lock on %s", key);
                     }
                     entry.wait();
                     continue;
                  } catch (InterruptedException e) {
                     Thread.currentThread().interrupt();
                     throw new IllegalStateException("Unexpected interruption!", e);
                  }
               }
               entry.update(file, offset);
               return;
            }
         } else {
            return;
         }
      }
   }

   public LockedEntry replaceOrLock(Object key, int file, int offset, int prevFile, int prevOffset) {
      for (;;) {
         Entry lockedEntry = new Entry(-1, -1, true);
         Entry entry = table.putIfAbsent(key, lockedEntry);
         if (entry != null) {
            synchronized (entry) {
               if (entry.isRemoved()) {
                  continue;
               }
               if (entry.isLocked()) {
                  throw new IllegalStateException("Unexpected double locking");
               }
               if (entry.getFile() == prevFile && entry.getOffset() == prevOffset) {
                  entry.update(file, offset);
               }
               return null;
            }
         } else {
            return lockedEntry;
         }
      }
   }

   public void updateAndUnlock(LockedEntry lockedEntry, int file, int offset) {
      Entry entry = (Entry) lockedEntry;
      synchronized (entry) {
         entry.file = file;
         entry.offset = offset;
         entry.locked = false;
         entry.notifyAll();
      }
   }

   public void removeAndUnlock(LockedEntry lockedEntry, Object key) {
      Entry entry = (Entry) lockedEntry;
      synchronized (entry) {
         table.remove(key);
         entry.setRemoved(true);
         entry.notifyAll();
      }
   }

   public EntryPosition get(Object key) {
      Entry entry = table.get(key);
      if (entry == null) {
         return null;
      }
      synchronized (entry) {
         // when the entry is locked, it means that it was not in the table before
         // and it's protected against writes, but its value is not up-to-date
         if (entry.isLocked()) {
            return null;
         }
         return new EntryPosition(entry.getFile(), entry.getOffset());
      }
   }

   public void clear() {
      table.clear();
   }

   public void removeConditionally(Object key, int file, int offset) {
      Entry tempEntry = table.get(key);
      if (tempEntry != null) {
         synchronized (tempEntry) {
            if (tempEntry.isLocked()) {
               return;
            }
            if (tempEntry.getFile() == file && tempEntry.getOffset() == offset) {
               table.remove(key, tempEntry);
               tempEntry.setRemoved(true);
            }
         }
      }
   }

   private static class Entry extends LockedEntry {
      private int file;
      private int offset;
      private boolean locked;
      private boolean removed = false;

      Entry(int file, int offset, boolean locked) {
         this.file = file;
         this.offset = offset;
         this.locked = locked;
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

      public boolean isLocked() {
         return locked;
      }
   }

   public abstract static class LockedEntry {
      private LockedEntry() {}
   }
}
