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

import javax.transaction.Transaction;
import java.util.Map;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class CountingCustomFailurePolicy extends AbstractCustomFailurePolicy {

   public static volatile boolean PUT_INVOKED;
   public static volatile boolean REMOVE_INVOKED;
   public static volatile boolean REPLACE_INVOKED;
   public static volatile boolean CLEAR_INVOKED;
   public static volatile boolean PUT_ALL_INVOKED;
   public static volatile boolean PREPARE_INVOKED;
   public static volatile boolean ROLLBACK_INVOKED;
   public static volatile boolean COMMIT_INVOKED;

   @Override
   public void handlePutFailure(String site, Object key, Object value, boolean putIfAbsent) {
      PUT_INVOKED = true;
   }

   @Override
   public void handleRemoveFailure(String site, Object key, Object oldValue) {
      REMOVE_INVOKED = true;
   }

   @Override
   public void handleReplaceFailure(String site, Object key, Object oldValue, Object newValue) {
      REPLACE_INVOKED = true;
   }

   @Override
   public void handleClearFailure(String site) {
      CLEAR_INVOKED = true;
   }

   @Override
   public void handlePutAllFailure(String site, Map map) {
      PUT_ALL_INVOKED = true;
   }

   @Override
   public void handlePrepareFailure(String site, Transaction transaction) {
      if (transaction == null)
         throw new IllegalStateException();
      PREPARE_INVOKED = true;
      throw new BackupFailureException();
   }

   @Override
   public void handleRollbackFailure(String site, Transaction transaction) {
      ROLLBACK_INVOKED = true;
   }

   @Override
   public void handleCommitFailure(String site, Transaction transaction) {
      COMMIT_INVOKED = true;
   }
}
