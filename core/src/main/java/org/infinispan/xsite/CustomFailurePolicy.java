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
 * Used for implementing custom policies in case of communication failures with a remote site. The handle methods are
 * allowed to throw instances of {@link BackupFailureException} to signal that they want the intra-site operation to
 * fail as well. If handle methods don't throw any exception then the operation will succeed in the local cluster. For
 * convenience, there is a support implementation of this class: {@link AbstractCustomFailurePolicy}
 * <p/>
 * Lifecycle: the same instance is invoked during the lifecycle of a cache so it is allowed to hold state between
 * invocations.
 * <p/>
 * Threadsafety: instances of this class might be invoked from different threads and they should be synchronized.
 *
 * @author Mircea Markus
 * @see BackupFailureException
 * @since 5.2
 */
public interface CustomFailurePolicy<K, V> {

   /**
    * Invoked during the initialization phase.
    */
   void init(Cache<K, V> cache);

   void handlePutFailure(String site, K key, V value, boolean putIfAbsent);

   void handleRemoveFailure(String site, K key, V oldValue);

   void handleReplaceFailure(String site, K key, V oldValue, V newValue);

   void handleClearFailure(String site);

   void handlePutAllFailure(String site, Map<K, V> map);

   void handlePrepareFailure(String site, Transaction transaction);

   void handleRollbackFailure(String site, Transaction transaction);

   void handleCommitFailure(String site, Transaction transaction);
}
