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

import org.infinispan.Cache;

import javax.transaction.Transaction;
import java.util.Map;

/**
 * Support class for {@link CustomFailurePolicy}.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public abstract class AbstractCustomFailurePolicy<K,V> implements CustomFailurePolicy<K,V> {

   protected volatile Cache<K,V> cache;

   @Override
   public void init(Cache cache) {
      this.cache = cache;
   }

   @Override
   public void handlePutFailure(String site, K key, V value, boolean putIfAbsent) {
   }

   @Override
   public void handleRemoveFailure(String site, K key, V oldValue) {
   }

   @Override
   public void handleReplaceFailure(String site, K key, V oldValue, V newValue) {
   }

   @Override
   public void handleClearFailure(String site) {
   }

   @Override
   public void handlePutAllFailure(String site, Map<K, V> map) {
   }

   @Override
   public void handlePrepareFailure(String site, Transaction transaction) {
   }

   @Override
   public void handleRollbackFailure(String site, Transaction transaction) {
   }

   @Override
   public void handleCommitFailure(String site, Transaction transaction) {
   }
}
