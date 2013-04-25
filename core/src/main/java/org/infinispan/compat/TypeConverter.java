/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.compat;

/**
 * A type converter for cached keys and values. Given a key and value type,
 * implementations of this interface convert instances of those types into
 * target key and value type instances respectively.
 *
 * @param <K> cached key type
 * @param <V> cached value type
 * @param <KT> target key type
 * @param <VT> target value type
 */
public interface TypeConverter<K, V, KT, VT> {

   // The reason this interface takes both key and value types into account
   // is because given a key type, implementations can make clever decisions
   // on how to convert value types into target value types, as seen in
   // boxValue and unboxValue method definitions.

   /**
    * Covert a instance of cached key type into an instance of target key type.
    *
    * @param key cached key instance to convert
    * @return a converted key instance into target key type
    */
   KT boxKey(K key);

   /**
    * Covert a instance of cached key type into an instance of target key type.
    *
    * @param key cached key associated with the value
    * @param value cached value instance to convert
    * @return a converted value instance into target value type
    */
   VT boxValue(K key, V value);

   /**
    * Convert back an instance of the target key type into an instance of the
    * cached key type.
    *
    * @param target target key type instance to convert back
    * @return an instance of the cached key type
    */
   K unboxKey(KT target);

   /**
    * Convert back an instance of the target value type into an instance of
    * the cached value type.
    *
    * @param key cached key associated with the value
    * @param target target value type instance to convert back
    * @return an instance of the cached value type
    */
   V unboxValue(K key, VT target);

}
