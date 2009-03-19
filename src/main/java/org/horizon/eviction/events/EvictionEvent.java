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
package org.horizon.eviction.events;

/**
 * An eviction event records activity on entries in the cache.  These are recorded for processing later.
 *
 * @author (various)
 * @since 1.0
 */
public class EvictionEvent {
   Object key;
   Type type;

   public static enum Type {
      ADD_ENTRY_EVENT,
      REMOVE_ENTRY_EVENT,
      VISIT_ENTRY_EVENT,
      CLEAR_CACHE_EVENT,
      MARK_IN_USE_EVENT,
      UNMARK_IN_USE_EVENT,
      EXPIRED_DATA_PURGE_START, // internal marker to denote when purging of expired data starts
      EXPIRED_DATA_PURGE_END // internal marker to denote when purging of expired data ends
   }

   public EvictionEvent(Object key, Type type) {
      this.key = key;
      this.type = type;
   }

   public Object getKey() {
      return key;
   }

   public void setKey(Object key) {
      this.key = key;
   }

   public void setEventType(Type event) {
      type = event;
   }

   public Type getEventType() {
      return type;
   }

   @Override
   public String toString() {
      return "EvictionEvent[key=" + key + " event=" + type + "]";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EvictionEvent that = (EvictionEvent) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (type != that.type) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (type != null ? type.hashCode() : 0);
      return result;
   }
}
