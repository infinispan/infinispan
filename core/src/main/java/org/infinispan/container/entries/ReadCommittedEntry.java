package org.infinispan.container.entries;

import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.container.entries.ReadCommittedEntry.Flags.CHANGED;
import static org.infinispan.container.entries.ReadCommittedEntry.Flags.COMMITTED;
import static org.infinispan.container.entries.ReadCommittedEntry.Flags.CREATED;
import static org.infinispan.container.entries.ReadCommittedEntry.Flags.EVICTED;
import static org.infinispan.container.entries.ReadCommittedEntry.Flags.EXPIRED;
import static org.infinispan.container.entries.ReadCommittedEntry.Flags.LOADED;
import static org.infinispan.container.entries.ReadCommittedEntry.Flags.REMOVED;
import static org.infinispan.container.entries.ReadCommittedEntry.Flags.SKIP_SHARED_STORE;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Util;
import org.infinispan.container.DataContainer;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A wrapper around a cached entry that encapsulates read committed semantics when writes are initiated, committed or
 * rolled back.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class ReadCommittedEntry implements MVCCEntry {
   private static final Log log = LogFactory.getLog(ReadCommittedEntry.class);
   private static final boolean trace = log.isTraceEnabled();

   protected Object key;
   protected Object value;
   protected long created = -1, lastUsed = -1;
   protected short flags = 0;
   protected Metadata metadata;
   protected PrivateMetadata internalMetadata;

   public ReadCommittedEntry(Object key, Object value, Metadata metadata) {
      this.key = key;
      this.value = value;
      this.metadata = metadata;
   }

   // if this or any MVCC entry implementation ever needs to store a boolean, always use a flag instead.  This is far
   // more space-efficient.  Note that this value will be stored in a byte, which means up to 8 flags can be stored in
   // a single byte.  Always start shifting with 0, the last shift cannot be greater than 7.
   protected enum Flags {
      CHANGED(1),
      CREATED(1 << 1),
      REMOVED(1 << 2),
      COMMITTED(1 << 3),
      EVICTED(1 << 4),
      EXPIRED(1 << 5),
      SKIP_LOOKUP(1 << 6),
      READ(1 << 7),
      LOADED(1 << 8),
      // Set if this write should not be persisted into any underlying shared stores
      SKIP_SHARED_STORE(1 << 9),
      ;

      final short mask;

      Flags(int mask) {
         this.mask = (short) mask;
      }
   }

   /**
    * Tests whether a flag is set.
    *
    * @param flag flag to test
    * @return true if set, false otherwise.
    */
   final boolean isFlagSet(Flags flag) {
      return (flags & flag.mask) != 0;
   }

   /**
    * Utility method that sets the value of the given flag to true.
    *
    * @param flag flag to set
    */
   protected final void setFlag(Flags flag) {
      flags |= flag.mask;
   }

   /**
    * Utility method that sets the value of the flag to false.
    *
    * @param flag flag to unset
    */
   private void unsetFlag(Flags flag) {
      flags &= ~flag.mask;
   }


   @Override
   public final long getLifespan() {
      return metadata == null ? -1 : metadata.lifespan();
   }

   @Override
   public final long getMaxIdle() {
      return metadata == null ? -1 : metadata.maxIdle();
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
   public boolean isNull() {
      return value == null;
   }

   @Override
   public final void commit(DataContainer container) {
      if (shouldCommit()) {
         if (isEvicted()) {
            container.evict(key);
         } else if (isRemoved()) {
            container.remove(key);
         } else if (value != null) {
            // Can't just rely on the entry's metadata because it could have
            // been modified by the interceptor chain (i.e. new version
            // generated if none provided by the user)
            container.put(key, value, metadata);
         }
      }
   }

   public final CompletionStage<Void> commit(int segment, InternalDataContainer container) {
      if (segment < 0) {
         throw new IllegalArgumentException("Segment must be 0 or greater");
      }
      // only do stuff if there are changes.
      if (shouldCommit()) {
         if (isEvicted()) {
            return container.evict(segment, key);
         } else if (isRemoved()) {
            container.remove(segment, key);
         } else if (value != null) {
            container.put(segment, key, value, metadata, internalMetadata, created, lastUsed);
         }
      }
      return CompletableFutures.completedNull();
   }

   private boolean shouldCommit() {
      if (isChanged()) {
         if (trace)
            log.tracef("Updating entry (key=%s removed=%s changed=%s created=%s value=%s metadata=%s internalMetadata=%s)",
                  toStr(getKey()), isRemoved(), isChanged(), isCreated(), toStr(value), getMetadata(), internalMetadata);
         return true;
      }
      return false;
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
   public void setSkipLookup(boolean skipLookup) {
      //no-op
   }

   @Override
   public boolean skipLookup() {
      //in read committed, it can read from the data container / remote source multiple times.
      return false;
   }

   @Override
   public long getCreated() {
      return created;
   }

   @Override
   public long getLastUsed() {
      return lastUsed;
   }

   @Override
   public Object setValue(Object value) {
      Object prev = this.value;
      this.value = value;
      return prev;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   @Override
   public final boolean isCreated() {
      return isFlagSet(CREATED);
   }

   @Override
   public final void setCreated(boolean created) {
      setFlag(created, CREATED);
   }

   @Override
   public boolean isRemoved() {
      return isFlagSet(REMOVED);
   }

   @Override
   public boolean isEvicted() {
      return isFlagSet(EVICTED);
   }

   @Override
   public boolean isExpired() {
      return isFlagSet(EXPIRED);
   }

   @Override
   public void setCommitted() {
      setFlag(COMMITTED);
   }

   @Override
   public boolean isCommitted() {
      return isFlagSet(COMMITTED);
   }

   @Override
   public boolean isLoaded() {
      return isFlagSet(LOADED);
   }

   @Override
   public boolean isSkipSharedStore() {
      return isFlagSet(SKIP_SHARED_STORE);
   }

   @Override
   public void setLoaded(boolean loaded) {
      setFlag(loaded, LOADED);
   }

   @Override
   public void resetCurrentValue() {
      // noop, the entry is removed from context
   }

   @Override
   public void updatePreviousValue() {
      // noop, the previous value is not stored
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
   public void setExpired(boolean expired) {
      setFlag(expired, EXPIRED);
   }

   @Override
   public void setSkipSharedStore() {
      setFlag(true, SKIP_SHARED_STORE);
   }

   final void setFlag(boolean enable, Flags flag) {
      if (enable)
         setFlag(flag);
      else
         unsetFlag(flag);
   }

   @Override
   public ReadCommittedEntry clone() {
      try {
         return (ReadCommittedEntry) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public void setCreated(long created) {
      this.created = created;
   }

   @Override
   public void setLastUsed(long lastUsed) {
      this.lastUsed = lastUsed;
   }

   @Override
   public PrivateMetadata getInternalMetadata() {
      return internalMetadata;
   }

   @Override
   public void setInternalMetadata(PrivateMetadata metadata) {
      this.internalMetadata = metadata;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "(" + Util.hexIdHashCode(this) + "){" +
            "key=" + toStr(key) +
            ", value=" + toStr(value) +
            ", isCreated=" + isCreated() +
            ", isChanged=" + isChanged() +
            ", isRemoved=" + isRemoved() +
            ", isExpired=" + isExpired() +
            ", skipLookup=" + skipLookup() +
            ", metadata=" + metadata +
            ", internalMetadata=" + internalMetadata +
            '}';
   }
}
