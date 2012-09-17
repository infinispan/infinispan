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

package org.infinispan.xsite;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.remoting.transport.BackupResponse;

import javax.transaction.Transaction;

/**
 * Component responsible with sending backup data to remote sites. The send operation is executed async, it's up to the
 * caller to wait on the returned {@link BackupResponse} in the case it wants an sync call.
 *
 * @see BackupResponse
 * @author Mircea Markus
 * @since 5.2
 */
public interface BackupSender {

   /**
    * Prepares a transaction on the remote site.
    */
   BackupResponse backupPrepare(PrepareCommand command) throws Exception;

   /**
    * Processes the responses of a backup command. It might throw an exception in the case the replication to the
    * remote site fail, based on the configured {@link CustomFailurePolicy}.
    */
   void processResponses(BackupResponse backupResponse, VisitableCommand command) throws Throwable;

   BackupResponse backupWrite(WriteCommand command) throws Exception;

   BackupResponse backupCommit(CommitCommand command) throws Exception;

   BackupResponse backupRollback(RollbackCommand command) throws Exception;

   void processResponses(BackupResponse backupResponse, VisitableCommand command, Transaction transaction) throws Throwable;
}
