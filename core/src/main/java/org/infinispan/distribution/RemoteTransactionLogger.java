/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import java.util.Collection;
import java.util.List;

/**
 * This abstraction performs RPCs and works on a TransactionLogger located on a remote node.
 *
 * @author Manik Surtani
 * @since 4.2.1
 */
public interface RemoteTransactionLogger {
   /**
    * Drains the transaction log and returns a list of what has been drained.
    *
    * @return a list of drained commands
    */
   List<WriteCommand> drain();

   /**
    * Similar to {@link #drain()} except that relevant locks are acquired so that no more commands are added to the
    * transaction log during this process, and transaction logging is disabled after draining.
    *
    * @return list of drained commands
    */
   List<WriteCommand> drainAndLock() throws InterruptedException;

   /**
    * Tests whether the drain() method can be called without a lock.  This is usually true if there is a lot of stuff to
    * drain.  After a certain threshold (once there are relatively few entries in the tx log) this will return false
    * after which you should call drainAndLock() to clear the final parts of the log.
    *
    * @return true if drain() should be called, false if drainAndLock() should be called.
    */
   boolean shouldDrainWithoutLock();

   /**
    * Drains pending prepares.  Note that this should *only* be done after calling drainAndLock() to prevent race
    * conditions
    *
    * @return a list of prepares pending commit or rollback
    */
   Collection<PrepareCommand> getPendingPrepares();

   /**
    * Unlocks and disables the transaction logger.  Should <i>only</i> be called after {@link #drainAndLock()}.
    */
   void unlockAndDisable();
}
