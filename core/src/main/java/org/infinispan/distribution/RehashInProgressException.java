/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.distribution;

import org.infinispan.CacheException;

/**
 * This exception is thrown when an operation cannot complete because a rehash is in progress.
 * Most of the time the operation will just wait for the rehash to complete and continue,
 * but if the rehash is taking too long this exception will be thrown.
 *
 * @author Dan Berindei <dberinde@redhat.com>
 */
public class RehashInProgressException extends CacheException {
   public RehashInProgressException() {
   }

   public RehashInProgressException(Throwable cause) {
      super(cause);
   }

   public RehashInProgressException(String msg) {
      super(msg);
   }

   public RehashInProgressException(String msg, Throwable cause) {
      super(msg, cause);
   }
}
