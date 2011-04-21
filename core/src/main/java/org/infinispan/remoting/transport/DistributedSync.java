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
package org.infinispan.remoting.transport;

import net.jcip.annotations.ThreadSafe;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This is an abstraction of a cluster-wide synchronization.  Its purpose is to maintain a set of locks that are aware
 * of block and unblock commands issued across a cluster.  In addition to these block and unblock phases, sub-phases
 * such as a start processing
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ThreadSafe
public interface DistributedSync {

   public enum SyncResponse {
      STATE_ACHIEVED, STATE_NOT_ACHIEVED, STATE_PREEXISTED
   }

   /**
    * Blocks until the start of a sync - either by the current RPCManager instance or a remote one - is received.  This
    * should return immediately if sync is already in progress.
    *
    * @param timeout  timeout after which to give up
    * @param timeUnit time unit
    * @throws TimeoutException if waiting for the sync times out.
    */
   SyncResponse blockUntilAcquired(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException;

   /**
    * Blocks until an ongoing sync ends.  This is returns immediately if there is no ongoing sync.
    *
    * @param timeout  timeout after which to give up
    * @param timeUnit time unit
    * @throws TimeoutException if waiting for an ongoing sync to end times out.
    */
   SyncResponse blockUntilReleased(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException;

   /**
    * Acquires the sync.  This could be from a local or remote source.
    */
   void acquireSync();

   /**
    * Releases the sync.  This could be from a local or remote source.
    */
   void releaseSync();

   /**
    * Acquires a processing lock.  This is typically acquired after the sync is acquired, and is meant for local (not
    * remote) use.
    *
    * @param exclusive whether the lock is exclusive
    * @param timeout   timeout after which to give up
    * @param timeUnit  time unit
    * @throws TimeoutException if waiting for the lock times out
    */
   void acquireProcessingLock(boolean exclusive, long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException;

   /**
    * Releases any processing locks that may be held by the current thread.
    */
   void releaseProcessingLock(boolean exclusive);
}
