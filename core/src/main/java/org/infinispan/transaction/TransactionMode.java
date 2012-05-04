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

package org.infinispan.transaction;

/**
 * Enumeration containing the available transaction modes for a cache.
 *
 * Starting with Infinispan version 5.1 a cache doesn't support mixed access:
 * i.e. won't support transactional and non-transactional operations.
 * A cache is transactional if one the following:
 *
 * <pre>
 * - a transactionManagerLookup is configured for the cache
 * - batching is enabled
 * - it is explicitly marked as transactional: config.fluent().transaction().transactionMode(TransactionMode.TRANSACTIONAL).
 *   In this last case a transactionManagerLookup needs to be explicitly set
 * </pre>
 *
 * By default a cache is not transactional.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public enum TransactionMode {
   NON_TRANSACTIONAL,
   TRANSACTIONAL;

   public boolean isTransactional() {
      return this == TRANSACTIONAL;
   }
}
