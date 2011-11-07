/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.context.impl;

import javax.transaction.Transaction;

import org.infinispan.transaction.AbstractCacheTransaction;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Support class for {@link org.infinispan.context.impl.TxInvocationContext}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class AbstractTxInvocationContext extends AbstractInvocationContext implements TxInvocationContext {

   private Transaction transaction;

   private boolean implicitTransaction;

   public boolean hasModifications() {
      return getModifications() != null && !getModifications().isEmpty();
   }

   public Set<Object> getAffectedKeys() {
      return getCacheTransaction().getAffectedKeys();
   }

   public void addAffectedKeys(Collection<Object> keys) {
      if (keys != null && !keys.isEmpty()) {
         getCacheTransaction().addAffectedKeys(keys);
      }
   }

   @Override
   public void addAffectedKey(Object key) {
      getCacheTransaction().addAffectedKey(key);
   }

   @Override
   public void setImplicitTransaction(boolean implicit) {
      this.implicitTransaction = implicit;
   }

   @Override
   public boolean isImplicitTransaction() {
      return this.implicitTransaction;
   }

   @Override
   public void reset() {
      super.reset();
      implicitTransaction = false;
   }

   public boolean isInTxScope() {
      return true;
   }

   public TxInvocationContext setTransaction(Transaction transaction) {
      this.transaction = transaction;
      return this;
   }

   public Transaction getTransaction() {
      return transaction;
   }

   @Override
   public final void clearLockedKeys() {
      getCacheTransaction().clearLockedKeys();
   }

   public abstract AbstractCacheTransaction getCacheTransaction();

}
