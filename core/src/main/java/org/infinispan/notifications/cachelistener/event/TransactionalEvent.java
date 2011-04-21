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
package org.infinispan.notifications.cachelistener.event;

import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * An event subtype that includes a transaction context - if one exists - as well as a boolean as to whether the call
 * originated locally or remotely.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface TransactionalEvent<K, V> extends Event<K, V> {
   /**
    * @return the Transaction associated with the current call.  May be null if the current call is outside the scope of
    *         a transaction.
    */
   GlobalTransaction getGlobalTransaction();

   /**
    * @return true if the call originated on the local cache instance; false if originated from a remote one.
    */
   boolean isOriginLocal();
}
