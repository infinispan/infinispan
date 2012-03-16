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
package org.infinispan.util.concurrent.locks;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.Lock;

/**
 * A lock that supports reentrancy based on owner (and not on current thread).  For this to work, the lock needs to be
 * constructed with a reference to the {@link org.infinispan.context.InvocationContextContainer}, so it is able to
 * determine whether the caller's "owner" reference is the current thread or a {@link
 * org.infinispan.transaction.xa.GlobalTransaction} instance.
 * <p/>
 * This makes this lock implementation very closely tied to Infinispan internals, but it provides for a very clean,
 * efficient and moreover familiar interface to work with, since it implements {@link java.util.concurrent.locks.Lock}.
 * <p/>
 * For the sake of performance, this lock only supports nonfair queueing.
 * <p/>
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@ThreadSafe
public class OwnableReentrantLock extends AbstractQueuedSynchronizer implements Lock {

   private static final long serialVersionUID = 4932974734462848792L;
   private transient Object owner;
   private final ThreadLocal<Object> requestorOnStack = new ThreadLocal<Object>();

   /**
    * @return a GlobalTransaction instance if the current call is participating in a transaction, or the current thread
    *         otherwise.
    */
   protected final Object currentRequestor() {
      Object cr = requestorOnStack.get();
      if (cr == null) throw new IllegalStateException("Should never get to this state!");
      return cr;
   }

   private void setCurrentRequestor(Object requestor) {
      requestorOnStack.set(requestor);
   }

   public void unsetCurrentRequestor() {
      requestorOnStack.remove();
   }

   public void lock() {
      throw new UnsupportedOperationException();
   }

   public void unlock() {
      throw new UnsupportedOperationException();
   }

   public void lock(GlobalTransaction requestor) {
      setCurrentRequestor(requestor);
      try {
         if (compareAndSetState(0, 1))
            owner = requestor;
         else
            acquire(1);
      } finally {
         unsetCurrentRequestor();
      }
   }

   public void lockInterruptibly() throws InterruptedException {
      acquireInterruptibly(1);
   }

   public boolean tryLock() {
      return tryAcquire(1);
   }

   public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
      throw new UnsupportedOperationException("Should never get here");
   }

   public boolean tryLock(Object requestor, long time, TimeUnit unit) throws InterruptedException {
      setCurrentRequestor(requestor);
      try {
         return tryAcquireNanos(1, unit.toNanos(time));
      } finally {
         unsetCurrentRequestor();
      }
   }

   public void unlock(Object requestor) {
      setCurrentRequestor(requestor);
      try {
         release(1);
      } catch (IllegalMonitorStateException imse) {
         // ignore?
      } finally {
         unsetCurrentRequestor();
      }
   }

   public ConditionObject newCondition() {
      throw new UnsupportedOperationException("Not supported in this implementation!");
   }

   @Override
   protected final boolean tryAcquire(int acquires) {
      final Object current = currentRequestor();
      int c = getState();
      if (c == 0) {
         if (compareAndSetState(0, acquires)) {
            owner = current;
            return true;
         }
      } else if (current.equals(owner)) {
         setState(c + acquires);
         return true;
      }
      return false;
   }

   @Override
   protected final boolean tryRelease(int releases) {
      int c = getState() - releases;
      if (!currentRequestor().equals(owner)) {
         //throw new IllegalMonitorStateException(this.toString());
         // lets be quiet about this
         return false;
      }
      boolean free = false;
      if (c == 0) {
         free = true;
         owner = null;
      }
      setState(c);
      return free;
   }

   @Override
   protected final boolean isHeldExclusively() {
      return getState() != 0 && currentRequestor().equals(owner);
   }

   /**
    * @return the owner of the lock, or null if it is currently unlocked.
    */
   public final Object getOwner() {
      int c = getState();
      Object o = owner;
      return (c == 0) ? null : o;
   }

   /**
    * @return the hold count of the current lock, or 0 if it is not locked.
    */
   public final int getHoldCount(Object requestor) {
      int c = getState();
      Object o = owner;
      return (requestor.equals(o)) ? c : 0;
   }

   /**
    * @return true if the lock is locked, false otherwise
    */
   public final boolean isLocked() {
      return getState() != 0;
   }

   /**
    * Reconstitute this lock instance from a stream, resetting the lock to an unlocked state.
    *
    * @param s the stream
    */
   private void readObject(java.io.ObjectInputStream s)
         throws java.io.IOException, ClassNotFoundException {
      s.defaultReadObject();
      setState(0); // reset to unlocked state
   }

   /**
    * Returns a string identifying this lock, as well as its lock state.  The state, in brackets, includes either the
    * String &quot;Unlocked&quot; or the String &quot;Locked by&quot; followed by the String representation of the lock
    * owner.
    *
    * @return a string identifying this lock, as well as its lock state.
    */
   public String toString() {
      Object owner = getOwner();
      return super.toString() + ((owner == null) ?
                                       "[Unlocked]" :
                                       "[Locked by " + owner + "]");
   }
}
