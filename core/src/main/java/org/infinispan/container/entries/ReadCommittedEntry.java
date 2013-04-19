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
package org.infinispan.container.entries;

import org.infinispan.atomic.AtomicHashMap;
import org.infinispan.container.DataContainer;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
   private long lifespan = -1;
   private long maxIdle = -1;

   protected ReadCommittedEntry() {
      setValid(true);
   }

   public ReadCommittedEntry(Object key, Object value, EntryVersion version, long lifespan) {
      setValid(true);
      this.key = key;
      this.value = value;
      this.lifespan = lifespan;
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
      LOADED(1 << 5);

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
      return lifespan;
   }

   @Override
   public final long getMaxIdle() {
      return maxIdle;
   }

   @Override
   public final void setMaxIdle(long maxIdle) {
      this.maxIdle = maxIdle;
   }

   @Override
   public final void setLifespan(long lifespan) {
      this.lifespan = lifespan;
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
   public void copyForUpdate(DataContainer container, boolean writeSkewCheck) {
      if (isChanged()) return; // already copied

      setChanged(true); // mark as changed

      // if newly created, then nothing to copy.
      if (!isCreated()) oldValue = value;
   }

   @Override
   public final void commit(DataContainer container, EntryVersion newVersion) {
      // TODO: No tombstones for now!!  I'll only need them for an eventually consistent cache

      // only do stuff if there are changes.
      if (isChanged() || isLoaded()) {
         if (trace)
            log.tracef("Updating entry (key=%s removed=%s valid=%s changed=%s created=%s loaded=%s value=%s]",
                  getKey(), isRemoved(), isValid(), isChanged(), isCreated(), isLoaded(), value);

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
            container.put(key, value, newVersion, lifespan, maxIdle);
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
   public boolean isValid() {
      return isFlagSet(VALID);
   }

   @Override
   public final void setValid(boolean valid) {
      setFlag(valid, VALID);
   }

   @Override
   public EntryVersion getVersion() {
      return null;
   }

   @Override
   public void setVersion(EntryVersion version) {
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

   private void setFlag(boolean enable, Flags flag) {
      if (enable)
         setFlag(flag);
      else
         unsetFlag(flag);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "(" + Util.hexIdHashCode(this) + "){" +
            "key=" + key +
            ", value=" + value +
            ", oldValue=" + oldValue +
            ", isCreated=" + isCreated() +
            ", isChanged=" + isChanged() +
            ", isRemoved=" + isRemoved() +
            ", isValid=" + isValid() +
            '}';
   }

   @Override
   public boolean undelete(boolean doUndelete) {
      if (isRemoved() && doUndelete) {
         if (trace) log.trace("Entry is deleted in current scope.  Un-deleting.");
         setRemoved(false);
         setValid(true);
         return true;
      }
      return false;
   }
}