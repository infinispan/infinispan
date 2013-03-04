/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.transaction.totalorder;

/**
 * Behaves as a latch between {@code org.infinispan.commands.tx.PrepareCommand} delivered in total order to coordinate
 * conflicting transactions and between {@code org.infinispan.commands.tx.PrepareCommand} and state transfer (blocking
 * the prepare until the state transfer is finished and blocking the state transfer until all the prepared transactions
 * has finished)
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public interface TotalOrderLatch {

   /**
    * @return true if this synchronization block is blocked
    */
   boolean isBlocked();

   /**
    * Unblocks this synchronization block
    */
   void unBlock();

   /**
    * It waits for this synchronization block to be unblocked.
    *
    * @throws InterruptedException if interrupted while waiting.
    */
   void awaitUntilUnBlock() throws InterruptedException;

}
