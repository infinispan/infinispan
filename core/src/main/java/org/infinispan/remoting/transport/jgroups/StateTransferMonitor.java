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
package org.infinispan.remoting.transport.jgroups;

import org.infinispan.statetransfer.StateTransferException;

public class StateTransferMonitor {
   /**
    * Reference to an exception that was raised during state installation on this cache.
    */
   protected volatile StateTransferException setStateException;
   private final Object stateLock = new Object();
   /**
    * True if state was initialized during start-up.
    */
   private volatile boolean isStateSet = false;

   public StateTransferException getSetStateException() {
      return setStateException;
   }

   public void waitForState() throws Exception {
      synchronized (stateLock) {
         while (!isStateSet) {
            if (setStateException != null) {
               throw setStateException;
            }

            try {
               stateLock.wait();
            }
            catch (InterruptedException iex) {
            }
         }
      }
   }

   public void notifyStateReceiptSucceeded() {
      synchronized (stateLock) {
         isStateSet = true;
         // Notify wait that state has been set.
         stateLock.notifyAll();
      }
   }

   public void notifyStateReceiptFailed(StateTransferException setStateException) {
      this.setStateException = setStateException;
      isStateSet = false;
      notifyStateReceiptSucceeded();
   }
}
