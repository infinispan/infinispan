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
package org.infinispan.cli.interpreter.statement;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.session.Session;

public class BeginTransactionStatement extends AbstractTransactionStatement {
   public BeginTransactionStatement(final String cacheName) {
      super(cacheName);
   }

   @Override
   public Result execute(Session session) throws StatementException {
      TransactionManager tm = getTransactionManager(session);
      if (tm==null) {
         throw new StatementException("Cannot retrieve a transaction manager for the cache");
      }
      try {
         tm.begin();
         return EmptyResult.RESULT;
      } catch (NotSupportedException e) {
         throw new StatementException("The TransactionManager does not support nested transactions");
      } catch (SystemException e) {
         throw new StatementException("Unexpected error while starting transaction: "+e.getMessage());
      }
   }
}
