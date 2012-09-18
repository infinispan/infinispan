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
package org.infinispan.transaction.lookup;

import javax.transaction.TransactionManager;

/**
 * Factory interface, allows {@link org.infinispan.Cache} to use different transactional systems. Names of implementors of
 * this class can be configured using {@link Configuration#setTransactionManagerLookupClass}.
 * Thread safety: it is possible for the same instance of this class to be used by multiple caches at the same time e.g.
 * when the same instance is passed to multiple configurations:
 * {@link org.infinispan.configuration.cache.TransactionConfigurationBuilder#transactionManagerLookup(TransactionManagerLookup)}.
 * As infinispan supports parallel test startup, it might be possible for multiple threads to invoke the
 * getTransactionManager() method concurrently, so it is highly recommended for instances of this class to be thread safe.
 *
 * @author Bela Ban, Aug 26 2003
 * @since 4.0
 */
public interface TransactionManagerLookup {

   /**
    * Returns a new TransactionManager.
    *
    * @throws Exception if lookup failed
    */
   TransactionManager getTransactionManager() throws Exception;

}
