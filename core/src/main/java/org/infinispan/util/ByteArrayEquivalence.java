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

package org.infinispan.util;

import java.util.Arrays;

/**
 * A compare function for unsigned byte arrays.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class ByteArrayEquivalence implements Equivalence<byte[]> {

   public static final Equivalence<byte[]> INSTANCE = new ByteArrayEquivalence();

   @Override
   public int hashCode(Object obj) {
      return 41 + Arrays.hashCode((byte[]) obj);
   }

   @Override
   public boolean equals(byte[] obj, Object otherObj) {
      if (obj == otherObj) return true;
      if (obj == null) return false;
      if (otherObj == null || byte[].class != otherObj.getClass()) return false;
      byte[] otherByteArray = (byte[]) otherObj;
      return Arrays.equals(obj, otherByteArray);
   }

   @Override
   public String toString(Object obj) {
      return Arrays.toString((byte[]) obj);
   }

   @Override
   public boolean isComparable(Object obj) {
      return true;
   }

   @Override
   public int compare(byte[] obj, byte[] otherObj) {
      // Assumes unsigned byte arrays
      int minLength = Math.min(obj.length, otherObj.length);
      for (int i = 0; i < minLength; i++) {
         int compareResult = obj[i] - otherObj[i];
         if (compareResult != 0)
            return compareResult;
      }
      return obj.length - otherObj.length;
   }

}
