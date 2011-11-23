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
package org.infinispan.api.util.concurrent;

import java.util.concurrent.Future;

/**
 * A sub-interface of a Future, that allows for listeners to be attached so that observers can be notified of when the
 * future completes.
 * <p/>
 * See {@link FutureListener} for more details.
 * <p/>
 * {@link #attachListener(FutureListener)} returns the same future instance, which is useful for 'building' a future.
 * E.g.,
 * <p/>
 * <code> Future<Void> f = cache.clearAsync().attachListener(new MyCustomListener()); </code>
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface NotifyingFuture<T> extends Future<T> {

   /**
    * Attaches a listener and returns the same future instance, to allow for 'building'.
    *
    * @param listener listener to attach
    * @return the same future instance
    */
   NotifyingFuture<T> attachListener(FutureListener<T> listener);

}
