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

import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.xsite.BackupSender;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class BaseBackupInterceptor extends BaseRpcInterceptor {

   protected BackupSender backupSender;
   protected TransactionTable txTable;

   @Inject
   void init(BackupSender sender, TransactionTable txTable) {
      this.backupSender = sender;
      this.txTable = txTable;
   }
}
