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
package org.infinispan.factories;

import javax.transaction.TransactionManager;

import org.infinispan.CacheException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.transaction.tm.BatchModeTransactionManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Uses a number of mechanisms to retrieve a transaction manager.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @since 4.0
 */
@DefaultFactoryFor(classes = {TransactionManager.class})
public class TransactionManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   private static final Log log = LogFactory.getLog(TransactionManagerFactory.class);

   @Override
   public <T> T construct(Class<T> componentType) {
      if (!configuration.transaction().transactionMode().isTransactional()) {
         return null;
      }

      // See if we had a TransactionManager injected into our config
      TransactionManager transactionManager = null;

      TransactionManagerLookup lookup = configuration.transaction().transactionManagerLookup();
      try {
         if (lookup != null) {
            componentRegistry.wireDependencies(lookup);
            transactionManager = lookup.getTransactionManager();
         }
      } catch (Exception e) {
         log.couldNotInstantiateTransactionManager(e);
      }

      if (transactionManager == null && configuration.invocationBatching().enabled()) {
         log.usingBatchModeTransactionManager();
         transactionManager = BatchModeTransactionManager.getInstance();
      }

      if (transactionManager == null) {
         throw new CacheException("This is transactional cache but no transaction manager could be found. " +
                                        "Configure the transaction manager lookup properly.");
      }

      return componentType.cast(transactionManager);
   }
}
