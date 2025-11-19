package org.infinispan.persistence.sifs;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntConsumer;

import org.infinispan.commons.util.IntSet;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Table holding the entry positions in log before these are persisted to the index.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class TemporaryTable {
   private static final Log log = Log.getLog(TemporaryTable.class);
   private final AtomicReferenceArray<ConcurrentMap<Object, Entry>> table;

   public TemporaryTable(int numSegments) {
      table = new AtomicReferenceArray<>(numSegments);
   }

   public int getSegmentMax() {
      return table.length();
   }

   public void addSegments(IntSet segments) {
      segments.forEach((IntConsumer) segment -> table.compareAndSet(segment, null, new ConcurrentHashMap<>()));
   }

   public void removeSegments(IntSet segments) {
      segments.forEach((IntConsumer) segment -> table.set(segment, null));
   }

   public boolean set(int segment, Object key, int file, int offset) {
      ConcurrentMap<Object, Entry> map = table.get(segment);
      if (map == null) {
         log.tracef("Table did not have segment %s", segment);
         return false;
      }
      for (; ; ) {
         Entry entry = map.putIfAbsent(key, new Entry(file, offset, false));
         if (entry != null) {
            synchronized (entry) {
               if (entry.isRemoved()) {
                  continue;
               } else if (entry.isLocked()) {
                  try {
                     if (log.isTraceEnabled()) {
                        log.tracef("Waiting for lock on %s", key);
                     }
                     entry.wait();
                     continue;
                  } catch (InterruptedException e) {
                     Thread.currentThread().interrupt();
                     throw new IllegalStateException("Unexpected interruption!", e);
                  }
               }
               log.tracef("Updated key to %s:%s", file, offset);
               entry.update(file, offset);
               break;
            }
         } else {
            log.tracef("Set key to %s:%s", file, offset);
            break;
         }
      }
      return true;
   }

   public LockedEntry replaceOrLock(int segment, Object key, int file, int offset, int prevFile, int prevOffset) {
      ConcurrentMap<Object, Entry> map = table.get(segment);
      if (map == null) {
         log.tracef("Table did not have segment %s", segment);
         return null;
      }
      for (;;) {
         Entry lockedEntry = new Entry(-1, -1, true);
         Entry entry = map.putIfAbsent(key, lockedEntry);
         if (entry != null) {
            synchronized (entry) {
               if (entry.isRemoved()) {
                  continue;
               }
               if (entry.isLocked()) {
                  throw new IllegalStateException("Unexpected double locking");
               }
               if (entry.getFile() == prevFile && entry.getOffset() == prevOffset) {
                  log.tracef("Updated entry for key %s to %s:%s from %s:%s", key, file, offset, prevFile, prevOffset);
                  entry.update(file, offset);
               } else {
                  log.tracef("Did not update entry for key %s as %s:%s does not match %s:%s", key, prevFile, prevOffset, entry.getFile(), entry.getOffset());
               }
               return null;
            }
         } else {
            log.tracef("Locked entry for key %s", key);
            return lockedEntry;
         }
      }
   }

   public void updateAndUnlock(LockedEntry lockedEntry, int file, int offset) {
      Entry entry = (Entry) lockedEntry;
      synchronized (entry) {
         log.tracef("Updating entry to %s:%s from %s:%s", file, offset, entry.file, entry.offset);
         entry.file = file;
         entry.offset = offset;
         entry.locked = false;
         entry.notifyAll();
      }
   }

   public void removeAndUnlock(LockedEntry lockedEntry, int segment, Object key) {
      Entry entry = (Entry) lockedEntry;
      synchronized (entry) {
         ConcurrentMap<Object, Entry> map = table.get(segment);
         if (map != null) {
            Entry removedEntry = map.remove(key);
            assert removedEntry == entry : "Removed entry " + removedEntry + " for key which didn't match " + lockedEntry;
            log.tracef("Removed and unlocking entry %s", entry);
         } else {
            log.tracef("Table did not have segment %s", segment);
         }
         entry.setRemoved(true);
         entry.notifyAll();
      }
   }

   public EntryPosition get(int segment, Object key) {
      ConcurrentMap<Object, Entry> map = table.get(segment);
      if (map == null) {
         log.tracef("Table did not have segment %s", segment);
         return null;
      }
      Entry entry = map.get(key);
      if (entry == null) {
         log.tracef("Key %s not present in temporary table", key);
         return null;
      }
      synchronized (entry) {
         // when the entry is locked, it means that it was not in the table before
         // and it's protected against writes, but its value is not up-to-date
         if (entry.isLocked()) {
            log.tracef("Key %s was present in temporary table with %s:%s, but locked", key, entry.getFile(),
                  entry.getOffset());
            return null;
         }
         log.tracef("Key %s was present in temporary table with %s:%s", key, entry.getFile(), entry.getOffset());
         return new EntryPosition(entry.getFile(), entry.getOffset());
      }
   }

   public void clear() {
      log.tracef("Clearing TemporaryTable");
      for (int i = 0; i < table.length(); ++i) {
         ConcurrentMap<Object, Entry> map = table.get(i);
         if (map != null) {
            map.clear();
         }
      }
   }

   public void removeConditionally(int segment, Object key, int file, int offset) {
      ConcurrentMap<Object, Entry> map = table.get(segment);
      if (map == null) {
         log.tracef("Table did not have segment %s", segment);
         return;
      }
      Entry tempEntry = map.get(key);
      if (tempEntry != null) {
         synchronized (tempEntry) {
            if (tempEntry.isLocked()) {
               log.tracef("Key %s was present in temporary table with %s:%s, but locked", key, tempEntry.getFile(),
                     tempEntry.getOffset());
               return;
            }
            if (tempEntry.getFile() == file && tempEntry.getOffset() == offset) {
               log.tracef("Removed Key %s was present in temporary table with %s:%s", key, file, offset);
               map.remove(key, tempEntry);
               tempEntry.setRemoved(true);
            } else {
               log.tracef("Key %s was present in temporary table with %s:%s, which doesn't match %s:%s", key, tempEntry.getFile(),
                     tempEntry.getOffset(), file, offset);
            }
         }
      } else {
         log.tracef("Table did contain key %s", key);
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

   <K, V> Flowable<Map.Entry<Object, EntryPosition>> publish(IntSet segments) {
      return Flowable.fromIterable(segments)
            .flatMap(segment -> {
               ConcurrentMap<Object, Entry> map = table.get(segment);
               if (map == null) {
                  return Flowable.empty();
               }
               return Flowable.fromIterable(map.entrySet())
                     .filter(entry -> !entry.getValue().isLocked())
                     .map(entry -> new AbstractMap.SimpleImmutableEntry<>(entry.getKey(),
                           new EntryPosition(entry.getValue().getFile(), entry.getValue().getOffset())));
            });
   }
}
