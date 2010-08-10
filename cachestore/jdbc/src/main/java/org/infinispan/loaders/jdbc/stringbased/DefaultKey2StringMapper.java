/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.loaders.jdbc.stringbased;

/**
 * Default implementation for {@link org.infinispan.loaders.jdbc.stringbased.Key2StringMapper}. It supports all the
 * primitive wrappers(e.g. Integer, Long etc).
 *
 * @author Mircea.Markus@jboss.com
 * @deprecated since 4.1 when {@link org.infinispan.loaders.jdbc.stringbased.DefaultTwoWayKey2StringMapper}
 * which is a replacement this implementation. 
 */
public class DefaultKey2StringMapper implements TwoWayKey2StringMapper {

   /**
    * Returns true if this is an primitive wrapper, false otherwise.
    */
   public boolean isSupportedType(Class key) {
      return DefaultTwoWayKey2StringMapper.isPrimitive(key);
   }

   @Override
   public Object getKeyMapping(String key) {
      return null;
   }

   /**
    * Returns key.toString. As key being a primitive wrapper, this will ensure that it is unique.
    */
   public String getStringMapping(Object key) {
      if (key == null) {
         throw new NullPointerException("Not supporting null keys");
      }
      return key.toString();
   }

}
