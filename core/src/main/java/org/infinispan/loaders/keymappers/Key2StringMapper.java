/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.keymappers;

/**
 * Defines the logic of mapping a key object to a String. This is required by certain cache stores, in order
 * to map each key to a String which the underlying store is capable of handling. It should generate a unique String
 * based on the supplied key.
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani
 */
public interface Key2StringMapper {

   /**
    * Do we support this key type?
    * @param keyType type to test
    * @return true if the type is supported, false otherwise.
    */
   boolean isSupportedType(Class<?> keyType);

   /**
    * Must return an unique String for the supplied key.
    * @param key key to map to a String
    * @return String representation of the key
    */
   String getStringMapping(Object key);
}
