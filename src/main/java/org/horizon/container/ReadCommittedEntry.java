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
package org.horizon.container;

import static org.horizon.container.ReadCommittedEntry.Flags.*;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;

/**
 * A wrapper around a cached entry that encapsulates read committed semantics when writes are initiated, committed or
 * rolled back.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class ReadCommittedEntry implements UpdateableEntry {
   private static final Log log = LogFactory.getLog(ReadCommittedEntry.class);
   private static final boolean trace = log.isTraceEnabled();

   protected Object key, value, oldValue;
   protected byte flags = 0;
   private long lifespan;

   protected ReadCommittedEntry() {
      setValid(true);
   }

   public ReadCommittedEntry(Object key, Object value, long lifespan) {
      setValid(true);
      this.key = key;
      this.value = value;
      this.lifespan = lifespan;
   }

   public long getLifespan() {
      return lifespan;
   }

   public void setLifespan(long lifespan) {
      this.lifespan = lifespan;
   }

   public Object getKey() {
      return key;
   }

   public Object getValue() {
      return value;
   }

   public Object setValue(Object value) {
      Object oldValue = this.value;
      this.value = value;
      return oldValue;
   }

   protected static enum Flags {
      CHANGED(1), CREATED(1 << 1), DELETED(1 << 2), VALID(1 << 3);
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

   public boolean isNullEntry() {
      return false;
   }

   public void copyForUpdate(DataContainer container, boolean writeSkewCheck) {
      if (isFlagSet(CHANGED)) return; // already copied

      setFlag(CHANGED);  // mark as changed

      // if newly created, then nothing to copy.
      if (!isFlagSet(CREATED)) oldValue = value;
   }

   @SuppressWarnings("unchecked")
   public void commitUpdate(DataContainer container) {
      // only do stuff if there are changes.
      if (isFlagSet(CHANGED)) {
         if (trace)
            log.trace("Updating entry [" + getKey() + "].  deleted=" + isDeleted() + " valid=" + isValid() + " changed=" + isChanged() + " created=" + isFlagSet(CREATED) + " value=" + value);
         if (isFlagSet(DELETED)) {
            container.remove(key);
         } else {
            container.put(key, value, lifespan);
         }
         reset();
      }
   }

   private void reset() {
      oldValue = null;
      flags = 0;
      setValid(true);
   }

   public void rollbackUpdate() {
      if (isFlagSet(CHANGED)) {
         value = oldValue;
         reset();
      }
   }

   public boolean isChanged() {
      return isFlagSet(CHANGED);
   }

   public boolean isValid() {
      return isFlagSet(VALID);
   }

   public void setValid(boolean valid) {
      if (valid)
         setFlag(VALID);
      else
         unsetFlag(VALID);
   }

   public boolean isCreated() {
      return isFlagSet(CREATED);
   }

   public void setCreated(boolean created) {
      if (created)
         setFlag(CREATED);
      else
         unsetFlag(CREATED);
   }

   public boolean isDeleted() {
      return isFlagSet(DELETED);
   }

   public void setDeleted(boolean deleted) {
      if (deleted)
         setFlag(DELETED);
      else
         unsetFlag(DELETED);
   }

   public String toString() {
      return getClass().getSimpleName() + "(" + System.identityHashCode(this) + "){" +
            "key=" + key +
            ", value=" + value +
            ", oldValue=" + oldValue +
            ", isCreated=" + isCreated() +
            ", isChanged=" + isChanged() +
            ", isDeleted=" + isDeleted() +
            ", isValid=" + isValid() +
            '}';
   }
}