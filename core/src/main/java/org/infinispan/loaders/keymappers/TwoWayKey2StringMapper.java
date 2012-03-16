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
 * Extends {@link Key2StringMapper} and allows a bidirectional transformation between keys and Strings.  Note that the
 * object instance created by {@link #getKeyMapping(String)} is guaranteed to be <i>equal</i> to the original object
 * used to generate the String, but not necessarily the same object reference.
 * <p />
 * The following condition should be satisfied by implementations of this interface:
 * <code>
 *   assert key.equals(mapper.getKeyMapping(mapper.getStringMapping(key)));
 * </code>
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani
 * @since 4.1
 */
public interface TwoWayKey2StringMapper extends Key2StringMapper {
   /**
    * Maps a String back to its original key
    * @param stringKey string representation of a key
    * @return an object instance that is <i>equal</i> to the original object used to create the key mapping.
    */
   Object getKeyMapping(String stringKey);
}
