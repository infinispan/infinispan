/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
 * This event subtype is passed in to any method annotated with {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved}.
 * <p />
 * The {@link #getValue()} method would return the <i>old</i> value prior to deletion, if <tt>isPre()</tt> is <tt>true</tt>.
 * If <tt>isPre()</tt> is <tt>false</tt>, {@link #getValue()} will return <tt>null</tt>.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheEntryRemovedEvent extends CacheEntryEvent {

   /**
    * Retrieves the value of the entry being deleted.
    * <p />
    * @return the value of the entry being deleted, if <tt>isPre()</tt> is <tt>true</tt>.  <tt>null</tt> otherwise.
    */
   Object getValue();
}
