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
package org.infinispan.transaction.xa.recovery;

import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.LocalXaTransaction;
import org.infinispan.util.Equivalence;

import javax.transaction.Transaction;

/**
 * Extends {@link org.infinispan.transaction.xa.LocalXaTransaction} and adds recovery related information.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryAwareLocalTransaction extends LocalXaTransaction implements RecoveryAwareTransaction {

   private boolean prepared;

   private boolean completionFailed;

   public RecoveryAwareLocalTransaction(Transaction transaction, GlobalTransaction tx,
         boolean implicitTransaction, int topologyId, Equivalence<Object> keyEquivalence) {
      super(transaction, tx, implicitTransaction, topologyId, keyEquivalence);
   }

   @Override
   public boolean isPrepared() {
      return prepared;
   }

   @Override
   public void setPrepared(boolean prepared) {
      this.prepared = prepared;
   }

   /**
    * Returns true if this transaction failed during 2nd phase of 2PC(prepare or commit). E.g. when the transaction successfully
    * prepared but the commit failed due to a network issue.
    */
   public boolean isCompletionFailed() {
      return completionFailed;
   }

   /**
    * @see  #isCompletionFailed()
    */
   public void setCompletionFailed(boolean completionFailed) {
      this.completionFailed = completionFailed;
   }
}
