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


import org.infinispan.container.versioning.EntryVersion;

/**
 * A null entry that is read in for removal
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class NullMarkerEntryForRemoval extends RepeatableReadEntry {

   public NullMarkerEntryForRemoval(Object key, EntryVersion version) {
      super(key, null, version, -1);
   }

   /**
    * @return always returns true
    */
   @Override
   public final boolean isNull() {
      return true;
   }

   /**
    * @return always returns true so that any get commands, upon getting this entry, will ignore the entry as though it
    *         were removed.
    */
   @Override
   public final boolean isRemoved() {
      return true;
   }

   /**
    * @return always returns true so that any get commands, upon getting this entry, will ignore the entry as though it
    *         were invalid.
    */
   @Override
   public final boolean isValid() {
      return false;
   }

}