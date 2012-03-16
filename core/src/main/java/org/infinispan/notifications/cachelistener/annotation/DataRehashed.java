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

import org.infinispan.config.Configuration;
import org.infinispan.notifications.IncorrectListenerException;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.notifications.cachelistener.event.Event;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation should be used on methods that need to be notified when a rehash starts or ends.  This is only fired
 * in a {@link Configuration.CacheMode#DIST_SYNC} or {@link Configuration.CacheMode#DIST_ASYNC} configured cache.
 * <p/>
 * Methods annotated with this annotation should accept a single parameter, a {@link DataRehashedEvent} otherwise a
 * {@link IncorrectListenerException} will be thrown when registering your listener.
 * <p/>
 * Note that methods marked with this annotation will be fired <i>before</i> and <i>after</i> rehashing takes place,
 * i.e., your method will be called twice, with {@link Event#isPre()} being set to <tt>true</tt> as well as <tt>false</tt>.
 * <p/>
 * Any exceptions thrown by the listener will abort the call. Any other listeners not yet called will not be called,
 * and any transactions in progress will be rolled back.
 *
 * @author Manik Surtani
 * @see org.infinispan.notifications.Listener
 * @since 5.0
 */
// ensure this annotation is available at runtime.
@Retention(RetentionPolicy.RUNTIME)
// ensure that this annotation is applied to classes.
@Target(ElementType.METHOD)
public @interface DataRehashed {
}
