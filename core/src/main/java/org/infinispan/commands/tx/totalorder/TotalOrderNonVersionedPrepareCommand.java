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

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.transaction.TotalOrderRemoteTransactionState;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.List;

/**
 * Command corresponding to the 1st phase of 2PC when Total Order based protocol is used. This command is used when non
 * versioned entries are needed.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderNonVersionedPrepareCommand extends PrepareCommand implements TotalOrderPrepareCommand {

   public static final byte COMMAND_ID = 38;

   public TotalOrderNonVersionedPrepareCommand(String cacheName, GlobalTransaction gtx, WriteCommand... modifications) {
      super(cacheName, gtx, true, modifications);
   }

   public TotalOrderNonVersionedPrepareCommand(String cacheName, GlobalTransaction gtx, List<WriteCommand> commands) {
      super(cacheName, gtx, commands, true);
   }

   public TotalOrderNonVersionedPrepareCommand(String cacheName) {
      super(cacheName);
   }

   private TotalOrderNonVersionedPrepareCommand() {
      super(null); // For command id uniqueness test
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void markAsOnePhaseCommit() {
      //no-op. it is always one phase commit
   }

   @Override
   public void markSkipWriteSkewCheck() {
      //no-op. no write skew check in non versioned mode
   }

   @Override
   public boolean skipWriteSkewCheck() {
      return true; //no write skew check with non versioned mode
   }

   @Override
   public TotalOrderRemoteTransactionState getOrCreateState() {
      return getRemoteTransaction().getTransactionState();
   }

}
