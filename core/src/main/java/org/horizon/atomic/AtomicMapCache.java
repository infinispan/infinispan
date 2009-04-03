/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.horizon.atomic;

import org.horizon.Cache;

/**
 * This interface adds the getAtomicMap() method which allows users to get a hold of a map type where operations on its
 * elements are all atomic.  Refer to the {@link AtomicMap} javadocs for more details.
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @see AtomicMap
 * @since 4.0
 */
public interface AtomicMapCache<K, V> extends Cache<K, V> {
   /**
    * Returns an atomic map.  The classes passed in are used to parameterize the Map returned.
    *
    * @param key          key under which to obtain and store this map in the cache
    * @param <AMK>          map keys
    * @param <AMV>          map values
    * @return a new or existing atomic map.  Never null.
    * @throws ClassCastException if there already is a value stored under the given key and the type of value cannot be
    *                            used as an AtomicMap.
    */
   <AMK, AMV> AtomicMap<AMK, AMV> getAtomicMap(K key) throws ClassCastException;
}
