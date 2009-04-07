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
package org.infinispan.atomic;

import java.util.Map;

/**
 * This is a special type of Map geared for use in Infinispan.  This map type supports Infinispan atomicizing writes
 * on the cache such that a coarse grained locking is used if this map is stored in the cache, such that the entire map
 * is locked for writes or is isolated for safe concurrent read.
 * <p/>
 * This is, for all practical purposes, a marker interface that indicates that Maps of this type will be locked
 * atomically in the cache and replicated in a fine grained manner.
 * <p/>
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @see DeltaAware
 * @see AtomicHashMap
 * @since 4.0
 */
public interface AtomicMap<K, V> extends Map<K, V> {
}
