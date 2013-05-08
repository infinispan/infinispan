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

package org.infinispan.interceptors.xsite;

import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.BackupSender;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class BaseBackupInterceptor extends CommandInterceptor {

   protected BackupSender backupSender;
   protected TransactionTable txTable;

   private static final Log log = LogFactory.getLog(BaseBackupInterceptor.class);
   
   @Inject
   void init(BackupSender sender, TransactionTable txTable) {
      this.backupSender = sender;
      this.txTable = txTable;
   }
   
   protected boolean isTxFromRemoteSite(GlobalTransaction gtx) {
      LocalTransaction remoteTx = txTable.getLocalTransaction(gtx);
      return remoteTx != null && remoteTx.isFromRemoteSite();
   }

   protected boolean shouldInvokeRemoteTxCommand(TxInvocationContext ctx) {
      // ISPN-2362: For backups, we should only replicate to the remote site if there are modifications to replay.
      boolean shouldBackupRemotely = ctx.isOriginLocal() && ctx.hasModifications();
      getLog().tracef("Should backup remotely? %s", shouldBackupRemotely);
      return shouldBackupRemotely;
   }
   
   @Override
   protected Log getLog() {
      return log;
   }
}
