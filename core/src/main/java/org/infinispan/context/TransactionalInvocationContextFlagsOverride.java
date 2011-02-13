/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
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

package org.infinispan.context;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.transaction.Transaction;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * Extension of InvocationContextFlagsOverride to be used when a TxInvocationContext
 * is required.
 * @see InvocationContextFlagsOverride
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 5.0
 */
public class TransactionalInvocationContextFlagsOverride extends InvocationContextFlagsOverride implements TxInvocationContext {
   
   private TxInvocationContext delegate;

   public TransactionalInvocationContextFlagsOverride(TxInvocationContext delegate, Set<Flag> flags) {
      super(delegate, flags);
      this.delegate = delegate;
   }

   @Override
   public boolean hasModifications() {
      return delegate.hasModifications();
   }

   @Override
   public Set<Object> getAffectedKeys() {
      return delegate.getAffectedKeys();
   }

   @Override
   public GlobalTransaction getGlobalTransaction() {
      return delegate.getGlobalTransaction();
   }

   @Override
   public List<WriteCommand> getModifications() {
      return delegate.getModifications();
   }

   @Override
   public Transaction getRunningTransaction() {
      return delegate.getRunningTransaction();
   }

   @Override
   public void addAffectedKeys(Collection<Object> keys) {
      delegate.addAffectedKeys(keys);
   }

   @Override
   public boolean isRunningTransactionValid() {
      return delegate.isRunningTransactionValid();
   }

}
