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

package org.infinispan.util.concurrent.locks;

import org.infinispan.util.concurrent.locks.containers.OwnableReentrantPerEntryLockContainer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A version of {@link OwnableReentrantLock} that has a reference counter, and implements {@link RefCountingLock}.
 * Used with a lock-per-entry container, in this case the {@link OwnableReentrantPerEntryLockContainer}.
 *
 * @author Manik Surtani
 * @since 5.2
 * @see OwnableReentrantPerEntryLockContainer
 */
public class OwnableRefCountingReentrantLock extends OwnableReentrantLock implements RefCountingLock {
   private final AtomicInteger references = new AtomicInteger(1);

   @Override
   public AtomicInteger getReferenceCounter() {
      return references;
   }

   @Override
   public String toString() {
      return super.toString() + "[References: "+references.toString()+"]";
   }
}
