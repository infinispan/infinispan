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

/**
 * This event subtype is passed in to any method annotated with {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryModified}
 * <p />
 * The {@link #getValue()} method's behavior is specific to whether the callback is triggered before or after the event
 * in question.  For example, if <tt>event.isPre()</tt> is <tt>true</tt>, then <tt>event.getValue()</tt> would return the
 * <i>old</i> value, prior to modification.  If <tt>event.isPre()</tt> is <tt>false</tt>, then <tt>event.getValue()</tt>
 * would return new <i>new</i> value.  If the event is creating and inserting a new entry, the old value would be <tt>null</tt>.
 * <p />
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheEntryModifiedEvent<K, V> extends CacheEntryEvent<K, V> {
   /**
    * Retrieves the value of the entry being modified.
    * <p />
    * @return the previous or new value of the entry, depending on whether isPre() is true or false.
    */
   V getValue();
}
