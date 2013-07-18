package org.infinispan.container.entries;

import org.infinispan.metadata.Metadata;
import org.infinispan.atomic.AtomicHashMap;
import org.infinispan.commons.util.Util;
import org.infinispan.container.DataContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.container.entries.ReadCommittedEntry.Flags.*;

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

   protected Object key, value, oldValue;
   protected byte flags = 0;
   protected Metadata metadata;

   public ReadCommittedEntry(Object key, Object value, Metadata metadata) {
      setValid(true);
      this.key = key;
      this.value = value;
      this.metadata = metadata;
   }

   @Override
   public byte getStateFlags() {
      return flags;
   }

   @Override
   public void copyStateFlagsFrom(StateChangingEntry other) {
      this.flags = other.getStateFlags();
   }

   // if this or any MVCC entry implementation ever needs to store a boolean, always use a flag instead.  This is far
   // more space-efficient.  Note that this value will be stored in a byte, which means up to 8 flags can be stored in
   // a single byte.  Always start shifting with 0, the last shift cannot be greater than 7.
   protected static enum Flags {
      CHANGED(1), // same as 1 << 0
      CREATED(1 << 1),
      REMOVED(1 << 2),
      VALID(1 << 3),
      EVICTED(1 << 4),
      LOADED(1 << 5),
      SKIP_REMOTE_GET(1 << 6),
      COPIED(1 << 7);

      final byte mask;

      Flags(int mask) {
         this.mask = (byte) mask;
      }
   }

   /**
    * Tests whether a flag is set.
    *
    * @param flag flag to test
    * @return true if set, false otherwise.
    */
   protected final boolean isFlagSet(Flags flag) {
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
   protected final void unsetFlag(Flags flag) {
      flags &= ~flag.mask;
   }


   @Override
   public final long getLifespan() {
      return metadata.lifespan();
   }

   @Override
   public final long getMaxIdle() {
      return metadata.maxIdle();
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
      this.value = value;
      return oldValue;
   }

   @Override
   public boolean isNull() {
      return false;
   }

   @Override
   public void copyForUpdate(DataContainer container) {
      if (isFlagSet(COPIED)) return; // already copied

      setFlag(COPIED); //mark as copied

      // if newly created, then nothing to copy.
      if (!isCreated()) oldValue = value;
   }

   @Override
   public final void commit(DataContainer container, Metadata providedMetadata) {
      // TODO: No tombstones for now!!  I'll only need them for an eventually consistent cache

      // only do stuff if there are changes.
      if (isChanged() || isLoaded()) {
         if (trace)
            log.tracef("Updating entry (key=%s removed=%s valid=%s changed=%s created=%s loaded=%s value=%s metadata=%s)",
                  toStr(getKey()), isRemoved(), isValid(), isChanged(), isCreated(), isLoaded(), toStr(value), getMetadata());

         // Ugh!
         if (value instanceof AtomicHashMap) {
            AtomicHashMap<?, ?> ahm = (AtomicHashMap<?, ?>) value;
            // Removing commit() call should not be an issue.
            // If marshalling is needed (clustering, or cache store), calling
            // delta() will clear the delta, avoiding leaking values in delta.
            // For local caches, using atomic hash maps does not make sense,
            // so leaking delta values is not so important.
            if (isRemoved() && !isEvicted()) ahm.markRemoved(true);
         }

         if (isRemoved()) {
            container.remove(key);
         } else if (value != null) {
            // Can't just rely on the entry's metadata because it could have
            // been modified by the interceptor chain (i.e. new version
            // generated if none provided by the user)
            container.put(key, value,
                  providedMetadata == null ? metadata : providedMetadata);
         }
         reset();
      }
   }

   private void reset() {
      oldValue = null;
      flags = 0;
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
   public void setSkipRemoteGet(boolean skipRemoteGet) {
      //no-op
   }

   @Override
   public boolean skipRemoteGet() {
      //in read committed, it can read from the data container / remote source multiple times.
      return false;
   }

   @Override
   public boolean isValid() {
      return isFlagSet(VALID);
   }

   @Override
   public final void setValid(boolean valid) {
      setFlag(valid, VALID);
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

   protected final void setFlag(boolean enable, Flags flag) {
      if (enable)
         setFlag(flag);
      else
         unsetFlag(flag);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "(" + Util.hexIdHashCode(this) + "){" +
            "key=" + toStr(key) +
            ", value=" + toStr(value) +
            ", oldValue=" + toStr(oldValue) +
            ", isCreated=" + isCreated() +
            ", isChanged=" + isChanged() +
            ", isRemoved=" + isRemoved() +
            ", isValid=" + isValid() +
            ", skipRemoteGet=" + skipRemoteGet() +
            ", metadata=" + metadata +
            '}';
   }

   @Override
   public boolean undelete(boolean doUndelete) {
      if (isRemoved() && doUndelete) {
         if (trace) log.trace("Entry is deleted in current scope.  Un-deleting.");
         setRemoved(false);
         setValid(true);
         setValue(null);
         return true;
      }
      return false;
   }
}
