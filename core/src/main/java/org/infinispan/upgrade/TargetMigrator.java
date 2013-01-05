/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.upgrade;

import org.infinispan.Cache;
import org.infinispan.CacheException;

/**
 * Performs migration operations on the target server or cluster of servers
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface TargetMigrator {
   /**
    * Returns the name of this migrator
    */
   String getName();
   /**
    * Performs the synchronization of data between source and target by retrieving the set of known keys and fetching each key in turn
    */
   long synchronizeData(Cache<Object, Object> cache) throws CacheException;

   /**
    * Disconnects the target from the source. This operation is the last step that must be performed after a rolling upgrade.
    */
   void disconnectSource(Cache<Object, Object> cache) throws CacheException;
}
