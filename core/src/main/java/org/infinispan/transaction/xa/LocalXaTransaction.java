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
package org.infinispan.transaction.xa;

import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.recovery.RecoverableTransactionIdentifier;
import org.infinispan.util.Equivalence;

import javax.transaction.Transaction;
import javax.transaction.xa.Xid;

/**
 * {@link LocalTransaction} implementation to be used with {@link TransactionXaAdapter}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class LocalXaTransaction extends LocalTransaction {

   private Xid xid;

   public LocalXaTransaction(Transaction transaction, GlobalTransaction tx,
         boolean implicitTransaction, int topologyId, Equivalence<Object> keyEquivalence) {
      super(transaction, tx, implicitTransaction, topologyId, keyEquivalence);
   }

   public void setXid(Xid xid) {
      this.xid  = xid;
      if (tx instanceof RecoverableTransactionIdentifier) {
         ((RecoverableTransactionIdentifier) tx).setXid(xid);
      }
   }

   public Xid getXid() {
      return xid;
   }

   /**
    * As per the JTA spec, XAResource.start is called on enlistment. That method also sets the xid for this local
    * transaction.
    */
   @Override
   public boolean isEnlisted() {
      return xid != null;
   }

   @Override
   public String toString() {
      return "LocalXaTransaction{" +
            "xid=" + xid +
            "} " + super.toString();
   }
}
