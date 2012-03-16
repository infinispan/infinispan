/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.notifications.cachelistener.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation should be used on methods that need to be notified when cache entries are passivated.
 * <p/>
 * Methods annotated with this annotation should accept a single parameter, a {@link
 * org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent} otherwise a {@link
 * org.infinispan.notifications.IncorrectListenerException} will be thrown when registering your listener.
 *  <p/>
 *  Locking: notification is performed WITH locks on the given key.
 * <p/>
 * Any exceptions thrown by the listener will abort the call. Any other listeners not yet called will not be called,
 * and any transactions in progress will be rolled back.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @see org.infinispan.notifications.Listener
 * @since 5.0
 */
// ensure this annotation is available at runtime.
@Retention(RetentionPolicy.RUNTIME)
// ensure that this annotation is applied to classes.
@Target(ElementType.METHOD)
public @interface CacheEntryPassivated {
}
