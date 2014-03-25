package org.infinispan.container.entries;

import org.infinispan.atomic.CopyableDeltaAware;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.atomic.AtomicHashMap;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commons.util.Util;
import org.infinispan.container.DataContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.LinkedList;
import java.util.List;

import static org.infinispan.container.entries.DeltaAwareCacheEntry.Flags.*;

/**
 * A wrapper around a cached entry that encapsulates DeltaAware and Delta semantics when writes are
 * initiated, committed or rolled back.
 * 
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 5.1
 */
public class DeltaAwareCacheEntry implements CacheEntry, StateChangingEntry {
   private static final Log log = LogFactory.getLog(DeltaAwareCacheEntry.class);
   private static final boolean trace = log.isTraceEnabled();

   protected Object key;
   protected CacheEntry wrappedEntry;
   protected DeltaAware value, oldValue;
   protected final List<Delta> deltas;
   protected byte flags = 0;

   // add Map representing uncommitted changes
   protected AtomicHashMap<?, ?> uncommittedChanges;

   public DeltaAwareCacheEntry(Object key, DeltaAware value, CacheEntry wrappedEntry) {
      setValid(true);
      this.key = key;
      this.value = value;
      this.wrappedEntry = wrappedEntry;
      if (value instanceof AtomicHashMap) {
         this.uncommittedChanges = ((AtomicHashMap) value).copy();
      }
      this.deltas = new LinkedList<Delta>();
   }

   @Override
   public byte getStateFlags() {
      if (wrappedEntry instanceof StateChangingEntry) {
         return ((StateChangingEntry)wrappedEntry).getStateFlags();
      }

      return flags;
   }

   @Override
   public void copyStateFlagsFrom(StateChangingEntry other) {
      this.flags = other.getStateFlags();
   }

   public void appendDelta(Delta d) {
      deltas.add(d);
      d.merge(uncommittedChanges);
      setChanged(true);
   }

   public AtomicHashMap<?, ?> getUncommittedChages() {
      return uncommittedChanges;
   }

   protected static enum Flags {
      CHANGED(1), // same as 1 << 0
      CREATED(1 << 1),
      REMOVED(1 << 2),
      VALID(1 << 3),
      EVICTED(1 << 4),
      LOADED(1 << 5),
      SKIP_LOOKUP(1 << 6);

      final byte mask;

      Flags(int mask) {
         this.mask = (byte) mask;
      }
   }

   /**
    * Tests whether a flag is set.
    * 
    * @param flag
    *           flag to test
    * @return true if set, false otherwise.
    */
   protected final boolean isFlagSet(Flags flag) {
      return (flags & flag.mask) != 0;
   }

   /**
    * Utility method that sets the value of the given flag to true.
    * 
    * @param flag
    *           flag to set
    */
   protected final void setFlag(Flags flag) {
      flags |= flag.mask;
   }

   /**
    * Utility method that sets the value of the flag to false.
    * 
    * @param flag
    *           flag to unset
    */
   protected final void unsetFlag(Flags flag) {
      flags &= ~flag.mask;
   }

   @Override
   public final long getLifespan() {
      return -1;  // forever
   }

   @Override
   public final long getMaxIdle() {
      return -1;  // forever
   }

   @Override
   public boolean skipLookup() {
      return isFlagSet(SKIP_LOOKUP);
   }

   @Override
   public final Object getKey() {
      return key;
   }

   @Override
   public final Object getValue() {
      return value;
   }

   @Override
   public final Object setValue(Object value) {
      Object oldValue = this.value;
      this.value = (DeltaAware) value;
      return oldValue;
   }

   @Override
   public boolean isNull() {
      return false;
   }

   @Override
   public final void commit(final DataContainer container, final Metadata metadata) {
      //If possible, we now ensure copy-on-write semantics. This way, it can ensure the correct transaction isolation.
      //note: we want to merge/copy to/from the data container value.
      container.compute(key, new DataContainer.ComputeAction() {
         @Override
         public InternalCacheEntry compute(Object key, InternalCacheEntry oldEntry, InternalEntryFactory factory) {
            InternalCacheEntry newEntry = oldEntry;
            DeltaAware containerValue = oldEntry == null ? null : (DeltaAware) oldEntry.getValue();
            if (containerValue != null && containerValue != value) {
               value = containerValue;
            }
            if (value != null && !deltas.isEmpty()) {
               final boolean makeCopy = value instanceof CopyableDeltaAware;
               if (makeCopy) {
                  value = ((CopyableDeltaAware) value).copy();
               }
               for (Delta delta : deltas) {
                  delta.merge(value);
               }
               if (makeCopy) {
                  //create or update existing entry.
                  newEntry = oldEntry == null ?
                        factory.create(key, value, extractMetadata(null, metadata)) :
                        factory.update(oldEntry, value, extractMetadata(oldEntry, metadata));
               }
               value.commit();
               if (wrappedEntry != null) {
                  wrappedEntry.setChanged(!makeCopy);
               }
            }
            reset();
            // only do stuff if there are changes.
            if (wrappedEntry != null) {
               wrappedEntry.commit(container, metadata);
            }
            return newEntry;
         }
      });
   }

   private Metadata extractMetadata(CacheEntry entry, Metadata provided) {
      if (provided != null) {
         return provided;
      } else if (wrappedEntry != null) {
         return wrappedEntry.getMetadata();
      }
      return entry == null ? null : entry.getMetadata();
   }

   private void reset() {
      oldValue = null;
      deltas.clear();
      flags = 0;
      if (uncommittedChanges != null) {
         uncommittedChanges.clear();
      }
      setValid(true);
   }

   @Override
   public final void rollback() {
      if (isChanged()) {
         value = oldValue;
         reset();
      }
   }

   @Override
   public final boolean isChanged() {
      return isFlagSet(CHANGED);
   }

   @Override
   public final void setChanged(boolean changed) {
      setFlag(changed, CHANGED);
   }

   @Override
   public boolean isValid() {
      if (wrappedEntry != null) {
         return wrappedEntry.isValid();
      } else {
         return isFlagSet(VALID);
      }
   }

   @Override
   public final void setValid(boolean valid) {
      setFlag(valid, VALID);
   }

   @Override
   public final boolean isCreated() {
      if (wrappedEntry != null) {
         return wrappedEntry.isCreated();
      } else {

         return isFlagSet(CREATED);
      }
   }

   @Override
   public final void setCreated(boolean created) {
      setFlag(created, CREATED);
   }

   @Override
   public boolean isRemoved() {
      if (wrappedEntry != null) {
         return wrappedEntry.isRemoved();
      } else {
         return isFlagSet(REMOVED);
      }
   }

   @Override
   public boolean isEvicted() {
      if (wrappedEntry != null) {
         return wrappedEntry.isEvicted();
      } else {
         return isFlagSet(EVICTED);
      }
   }

   @Override
   public final void setRemoved(boolean removed) {
      setFlag(removed, REMOVED);
   }

   @Override
   public void setEvicted(boolean evicted) {
      setFlag(evicted, EVICTED);
   }

   @Override
   public boolean isLoaded() {
      return isFlagSet(LOADED);
   }

   @Override
   public void setLoaded(boolean loaded) {
      setFlag(loaded, LOADED);
   }

   @Override
   public void setSkipLookup(boolean skipLookup) {
      setFlag(skipLookup, SKIP_LOOKUP);
   }

   private void setFlag(boolean enable, Flags flag) {
      if (enable)
         setFlag(flag);
      else
         unsetFlag(flag);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "(" + Util.hexIdHashCode(this) + "){" + "key=" + key
               + ", value=" + value + ", oldValue=" + uncommittedChanges + ", isCreated="
               + isCreated() + ", isChanged=" + isChanged() + ", isRemoved=" + isRemoved()
               + ", isValid=" + isValid() + '}';
   }

   @Override
   public boolean undelete(boolean doUndelete) {
      if (isRemoved() && doUndelete) {
         if (trace)
            log.trace("Entry is deleted in current scope.  Un-deleting.");
         setRemoved(false);
         setValid(true);
         return true;
      }
      return false;
   }

   @Override
   public Metadata getMetadata() {
      return null;  // DeltaAware are always metadata unaware
   }

   @Override
   public void setMetadata(Metadata metadata) {
      // DeltaAware are always metadata unaware
   }

}
