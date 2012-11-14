/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.util.concurrent;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public abstract class BaseNotifyingFuture<T> implements NotifyingFuture<T> {

   final Set<FutureListener<T>> listeners = new CopyOnWriteArraySet<FutureListener<T>>();
   volatile boolean callCompleted = false;
   final ReadWriteLock listenerLock = new ReentrantReadWriteLock();


   public final NotifyingFuture<T> attachListener(FutureListener<T> objectFutureListener) {
      listenerLock.readLock().lock();
      try {
         if (!callCompleted) listeners.add(objectFutureListener);
         if (callCompleted) objectFutureListener.futureDone(this);
         return this;
      } finally {
         listenerLock.readLock().unlock();
      }
   }

   public void notifyDone() {
      listenerLock.writeLock().lock();
      try {
         callCompleted = true;
         for (FutureListener<T> l : listeners) l.futureDone(this);
      } finally {
         listenerLock.writeLock().unlock();
      }
   }
}
