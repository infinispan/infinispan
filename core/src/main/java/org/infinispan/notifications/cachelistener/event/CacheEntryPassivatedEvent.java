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
package org.infinispan.notifications.cachelistener.event;

/**
 * This event subtype is passed in to any method annotated with
 * {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated}.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 5.0
 */
public interface CacheEntryPassivatedEvent<K, V> extends CacheEntryEvent<K, V> {
   /**
    * Retrieves the value of the entry being passivated.
    * <p />
    * @return the value of the entry being passivated, if <tt>isPre()</tt> is <tt>true</tt>.  <tt>null</tt> otherwise.
    */
   V getValue();
}
