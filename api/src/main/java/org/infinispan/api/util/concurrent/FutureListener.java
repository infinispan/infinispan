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
 * A listener that is called back when a future is done.  FutureListener instances are attached to {@link
 * NotifyingFuture}s by passing them in to {@link NotifyingFuture#attachListener(FutureListener)}
 * <p/>
 * Note that the {@link #futureDone(Future)} callback is invoked when the future completes, regardless of how the future
 * completes (i.e., normally, due to an exception, or cancelled}.  As such, implementations should check the future
 * passed in by calling <tt>future.get()</tt>.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface FutureListener<T> {
   void futureDone(Future<T> future);
}
