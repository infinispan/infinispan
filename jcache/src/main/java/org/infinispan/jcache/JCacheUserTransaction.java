/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.jcache;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

/**
 * A {@link UserTransaction} facade for JCache's requirement to provide
 * access to a user transaction implementation. It delegates to the
 * transaction manager.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class JCacheUserTransaction implements UserTransaction {

   private final TransactionManager tm;

   public JCacheUserTransaction(TransactionManager tm) {
      this.tm = tm;
   }

   @Override
   public void begin() throws NotSupportedException, SystemException {
      tm.begin();
   }

   @Override
   public void commit() throws RollbackException, HeuristicMixedException,
         HeuristicRollbackException, SecurityException,
         IllegalStateException, SystemException {
      tm.commit();
   }

   @Override
   public void rollback() throws IllegalStateException, SecurityException, SystemException {
      tm.rollback();
   }

   @Override
   public void setRollbackOnly() throws IllegalStateException, SystemException {
      tm.setRollbackOnly();
   }

   @Override
   public int getStatus() throws SystemException {
      if (tm == null)
         return Status.STATUS_NO_TRANSACTION;

      return tm.getStatus();
   }

   @Override
   public void setTransactionTimeout(int seconds) throws SystemException {
      tm.setTransactionTimeout(seconds);
   }

}
