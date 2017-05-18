package org.infinispan.container.entries;

import static org.infinispan.container.entries.DeltaAwareCacheEntry.Flags.CHANGED;
import static org.infinispan.container.entries.DeltaAwareCacheEntry.Flags.CREATED;
import static org.infinispan.container.entries.DeltaAwareCacheEntry.Flags.EVICTED;
import static org.infinispan.container.entries.DeltaAwareCacheEntry.Flags.EXPIRED;
import static org.infinispan.container.entries.DeltaAwareCacheEntry.Flags.REMOVED;
import static org.infinispan.container.entries.DeltaAwareCacheEntry.Flags.SKIP_LOOKUP;
import static org.infinispan.container.entries.DeltaAwareCacheEntry.Flags.VALID;

import java.util.LinkedList;
import java.util.List;

import org.infinispan.atomic.CopyableDeltaAware;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commons.util.Util;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A wrapper around a cached entry that encapsulates DeltaAware and Delta semantics when writes are
 * initiated, committed or rolled back.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 5.1
 */
// TODO: this class is going to work only for atomic hash maps as it has special handling for them
public class DeltaAwareCacheEntry<K> implements CacheEntry<K, DeltaAware>, MVCCEntry<K, DeltaAware> {
   private static final Log log = LogFactory.getLog(DeltaAwareCacheEntry.class);
   private static final boolean trace = log.isTraceEnabled();

   // TODO: this is hack!
   private InvocationContext ctx;
   private PersistenceManager persistenceManager;
   private TimeService timeService;

   protected K key;
   protected CacheEntry<K, DeltaAware> wrappedEntry;
   protected DeltaAware value;
   protected DeltaAware initialValue;
   protected final List<Delta> deltas;
   protected byte flags = 0;

   public DeltaAwareCacheEntry(K key, DeltaAware value, CacheEntry<K, DeltaAware> wrappedEntry,
                               InvocationContext ctx, PersistenceManager persistenceManager, TimeService timeService) {
      setValid(true);
      this.key = key;
      this.value = value;
      this.initialValue = value;
      this.wrappedEntry = wrappedEntry;
      this.ctx = ctx;
      this.persistenceManager = persistenceManager;
      this.timeService = timeService;
      this.deltas = new LinkedList<Delta>();
   }

   public void appendDelta(Delta d) {
      deltas.add(d);
      setChanged(true);
   }

   protected static enum Flags {
      CHANGED(1), // same as 1 << 0
      CREATED(1 << 1),
      REMOVED(1 << 2),
      VALID(1 << 3),
      EVICTED(1 << 4),
      SKIP_LOOKUP(1 << 6),
      EXPIRED(1 << 7);

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
   public final K getKey() {
      return key;
   }

   @Override
   public final DeltaAware getValue() {
      // TODO: it's not possible to return the actually modified (but uncommitted) value
      // when this is not a copyable entry
      return value;
   }

   @Override
   public final DeltaAware setValue(DeltaAware value) {
      DeltaAware oldValue = getValue();
      this.value = value;
      DeltaAware newValueCopy;
      newValueCopy = value;
      this.deltas.clear();
      // add a delta setting the value without caring about previous one
      this.deltas.add(d -> newValueCopy);
      return oldValue;
   }

   @Override
   public boolean isNull() {
      return false;
   }

   @Override
   public final void commit(final DataContainer<K, DeltaAware> container) {
      //If possible, we now ensure copy-on-write semantics. This way, it can ensure the correct transaction isolation.
      //note: we want to merge/copy to/from the data container value.
      PersistenceUtil.loadAndComputeInDataContainer(container, persistenceManager, key, ctx, timeService, (key, oldEntry, factory) -> {
            InternalCacheEntry<K, DeltaAware> newEntry = oldEntry;
            DeltaAware containerValue = oldEntry == null ? null : (DeltaAware) oldEntry.getValue();
            if (containerValue != null && containerValue != value) {
               value = containerValue;
            }
            // If we were removed then only apply the deltas
            if (!deltas.isEmpty()) {
               // If we were created don't use the original value
               if (isCreated()) {
                  value = null;
                  for (Delta delta : deltas) {
                     value = delta.merge(value);
                  }
                  value.commit();
                  newEntry = factory.create(key, value, extractMetadata(null));
               } else if (value != null) {
                  final boolean makeCopy = value instanceof CopyableDeltaAware;
                  if (makeCopy) {
                     value = ((CopyableDeltaAware) value).copy();
                  }
                  for (Delta delta : deltas) {
                     value = delta.merge(value);
                  }
                  if (makeCopy) {
                     //create or update existing entry.
                     newEntry = oldEntry == null ?
                             factory.create(key, value, extractMetadata(null)) :
                             factory.update(oldEntry, value, extractMetadata(oldEntry));
                  }
                  value.commit();
               }
            }
            return newEntry;
         });
   }

   private Metadata extractMetadata(CacheEntry<K, DeltaAware> entry) {
      if (wrappedEntry != null) {
         return wrappedEntry.getMetadata();
      }
      return entry == null ? null : entry.getMetadata();
   }

   @Override
   public void resetCurrentValue() {
      deltas.clear();
      flags = 0;
      setValid(true);
   }

   @Override
   public void updatePreviousValue() {
      // noop
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
   public void setExpired(boolean expired) {
      setFlag(expired, EXPIRED);
   }

   @Override
   public boolean isExpired() {
      return isFlagSet(EXPIRED);
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
      return false;
   }

   @Override
   public void setLoaded(boolean loaded) {
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
               + ", value=" + value + ", isCreated="
               + isCreated() + ", isChanged=" + isChanged() + ", isRemoved=" + isRemoved()
               + ", isValid=" + isValid() + '}';
   }

   @Override
   public Metadata getMetadata() {
      return null;  // DeltaAware are always metadata unaware
   }

   @Override
   public void setMetadata(Metadata metadata) {
      // DeltaAware are always metadata unaware
   }

   @Override
   public DeltaAwareCacheEntry<K> clone() {
      try {
         return (DeltaAwareCacheEntry) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new AssertionError();
      }
   }

}
