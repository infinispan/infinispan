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
package org.infinispan;

/**
 * Thrown when operations on {@link Cache} fail unexpectedly.
 * <p/>
 * Specific subclasses such as {@link org.infinispan.util.concurrent.TimeoutException} and {@link
 * org.infinispan.config.ConfigurationException} have more specific uses.
 * <p/>
 * Transactions: if a CacheException (including any subclasses) is thrown for an operation on a JTA transaction, then
 * the transaction is marked for rollback. 
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a>
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class CacheException extends RuntimeException {

   private static final long serialVersionUID = -4386393072593859164L;

   public CacheException() {
      super();
   }

   public CacheException(Throwable cause) {
      super(cause);
   }

   public CacheException(String msg) {
      super(msg);
   }

   public CacheException(String msg, Throwable cause) {
      super(msg, cause);
   }
}