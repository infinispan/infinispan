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

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.transaction.Transaction;
import java.security.PrivateKey;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Interface defining additional functionality for invocation contexts that propagate within a transaction's scope.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface TxInvocationContext extends InvocationContext {

   /**
    * Were there any modifications performed within the tx's scope?
    */
   public boolean hasModifications();

   /**
    * Returns the set of keys that are affected by this transaction.  Used to generate appropriate recipient groups
    * for cluster-wide prepare and commit calls.
    */
   Set<Object> getAffectedKeys();

   /**
    * Returns the id of the transaction associated  with the current call.
    */
   GlobalTransaction getGlobalTransaction();

   /**
    * Returns all the modifications performed in the scope of the current transaction.
    */
   List<WriteCommand> getModifications();

   /**
    * Returns the tx associated with the current thread. This method MUST be guarded with a call to {@link
    * #isOriginLocal()}, as {@link javax.transaction.Transaction} are not propagated from the node where tx was
    * started.
    * @throws IllegalStateException if the call is performed from a {@link #isOriginLocal()}==false context.
    */
   Transaction getTransaction();

   /**
    * Registers a new participant with the transaction.
    */
   void addAffectedKeys(Collection<Object> keys);

   void addAffectedKey(Object keys);

   /**
    *
    * @return true if the current transaction is in a valid state to perform operations on (i.e.,RUNNING or PREPARING)
    * or false otherwise.
    */
   boolean isTransactionValid();

   /**
    * Marks this transaction as implicit; implicit transactions are started for transactional caches that have the autoCommit enabled.
    */
   void setImplicitTransaction(boolean implicit);


   boolean isImplicitTransaction();

   CacheTransaction getCacheTransaction();
}
