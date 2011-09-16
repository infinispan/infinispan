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
package org.infinispan.transaction.xa;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.util.BidirectionalMap;

import java.util.List;

/**
 * Defines the state a infinispan transaction should have.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface CacheTransaction {

   /**
    * Returns the transaction identifier.
    */
   public GlobalTransaction getGlobalTransaction();

   /**
    * Returns the modifications visible within the current transaction.
    */
   public List<WriteCommand> getModifications();


   public CacheEntry lookupEntry(Object key);

   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries();

   public void putLookedUpEntry(Object key, CacheEntry e);

   public void removeLookedUpEntry(Object key);

   public void clearLookedUpEntries();

   public abstract boolean ownsLock(Object key);

}
