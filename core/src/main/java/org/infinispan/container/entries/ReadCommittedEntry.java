/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import org.infinispan.container.DataContainer;
import static org.infinispan.container.entries.ReadCommittedEntry.Flags.*;
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

   protected Object key, value, oldValue;
   protected byte flags = 0;
   private long lifespan = -1;
   private long maxIdle = -1;

   protected ReadCommittedEntry() {
      setValid(true);
   }

   public ReadCommittedEntry(Object key, Object value, long lifespan) {
      setValid(true);
      this.key = key;
      this.value = value;
      this.lifespan = lifespan;
   }

   protected static enum Flags {
      CHANGED(1), CREATED(1 << 1), REMOVED(1 << 2), VALID(1 << 3);
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
    * Unility method that sets the value of the given flag to true.
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


   public final long getLifespan() {
      return lifespan;
   }

   public final long getMaxIdle() {
      return maxIdle;
   }

   public final MVCCEntry setMaxIdle(long maxIdle) {
      this.maxIdle = maxIdle;
      return this;
   }

   public final MVCCEntry setLifespan(long lifespan) {
      this.lifespan = lifespan;
      return this;
   }

   public final Object getKey() {
      return key;
   }

   public final Object getValue() {
      return value;
   }

   public final Object setValue(Object value) {
      Object oldValue = this.value;
      this.value = value;
      return oldValue;
   }

   public boolean isNull() {
      return false;
   }

   public void copyForUpdate(DataContainer container, boolean writeSkewCheck) {
      if (isChanged()) return; // already copied

      setChanged(); // mark as changed

      // if newly created, then nothing to copy.
      if (!isCreated()) oldValue = value;
   }

   @SuppressWarnings("unchecked")
   public final void commit(DataContainer container) {
      // only do stuff if there are changes.
      if (isChanged()) {
         if (trace)
            log.trace("Updating entry (key={0} removed={1} valid={2} changed={3} created={4} value={5}]", getKey(),
                      isRemoved(), isValid(), isChanged(), isCreated(), value);
         if (isRemoved()) {
            container.remove(key);
         } else {
            container.put(key, value, lifespan, maxIdle);
         }
         reset();
      }
   }

   private void reset() {
      oldValue = null;
      flags = 0;
      setValid(true);
   }

   public final void rollback() {
      if (isChanged()) {
         value = oldValue;
         reset();
      }
   }

   public final boolean isChanged() {
      return isFlagSet(CHANGED);
   }

   protected final void setChanged() {
      setFlag(CHANGED);
   }

   public boolean isValid() {
      return isFlagSet(VALID);
   }

   public final void setValid(boolean valid) {
      if (valid)
         setFlag(VALID);
      else
         unsetFlag(VALID);
   }

   public final boolean isCreated() {
      return isFlagSet(CREATED);
   }

   public final void setCreated(boolean created) {
      if (created)
         setFlag(CREATED);
      else
         unsetFlag(CREATED);
   }

   public boolean isRemoved() {
      return isFlagSet(REMOVED);
   }

   public final void setRemoved(boolean removed) {
      if (removed)
         setFlag(REMOVED);
      else
         unsetFlag(REMOVED);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "(" + System.identityHashCode(this) + "){" +
            "key=" + key +
            ", value=" + value +
            ", oldValue=" + oldValue +
            ", isCreated=" + isCreated() +
            ", isChanged=" + isChanged() +
            ", isRemoved=" + isRemoved() +
            ", isValid=" + isValid() +
            '}';
   }
}