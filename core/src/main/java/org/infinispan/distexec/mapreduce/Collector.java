/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distexec.mapreduce;

/**
 * OutputCollector is the intermediate key/value result data output collector given to each {@link Mapper}
 * 
 * @see Mapper#map(Object, Object, Collector)
 * 
 * @author Mircea Markus
 * @author Sanne Grinovero
 * @since 5.0
 */
public interface Collector<K, V> {

   /**
    * Intermediate key/value callback used by {@link Mapper} implementor
    * 
    * @param key
    *           intermediate key
    * @param value
    *           intermediate value
    */
   void emit(K key, V value);

}
