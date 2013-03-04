/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

package org.infinispan.commands.tx.totalorder;

import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.transaction.TotalOrderRemoteTransactionState;

/**
 * Interface with the utilities methods that the prepare command must have when Total Order based protocol is used
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public interface TotalOrderPrepareCommand extends TransactionBoundaryCommand {

   /**
    * marks the prepare phase as 1PC to apply immediately the modifications. It is used when the {@code
    * org.infinispan.commands.tx.CommitCommand} is received before the {@code org.infinispan.commands.tx.PrepareCommand}.
    */
   void markAsOnePhaseCommit();

   /**
    * it signals that the write skew check is not needed (for versioned entries). It is used when the {@code
    * org.infinispan.commands.tx.CommitCommand} is received before the {@code org.infinispan.commands.tx.PrepareCommand}.
    */
   void markSkipWriteSkewCheck();

   /**
    * @return {@code true} when the write skew check is not needed.
    */
   boolean skipWriteSkewCheck();

   /**
    * @return the modifications performed by this transaction
    */
   WriteCommand[] getModifications();

   /**
    * returns the {@link TotalOrderRemoteTransactionState} associated with this transaction, creating one if no one was
    * associated to this transaction.
    *
    * @return returns the {@link TotalOrderRemoteTransactionState} associated with this transaction.
    */
   TotalOrderRemoteTransactionState getOrCreateState();
}
