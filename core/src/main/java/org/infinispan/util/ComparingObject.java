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

/**
 * A compare function for objects.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public enum ComparingObject implements Comparing {

   INSTANCE;

   @Override
   public int hashCode(Object obj) {
      return obj.hashCode();
   }

   @Override
   public boolean equals(Object obj, Object otherObj) {
      return obj != null && obj.equals(otherObj);
   }

   @Override
   public String toString(Object obj) {
      return obj.toString();
   }

   @Override
   public boolean isComparable(Object obj) {
      return obj instanceof Comparable;
   }

   @Override
   @SuppressWarnings("unchecked")
   public int compare(Object obj, Object otherObj) {
      return ((Comparable<Object>) obj).compareTo(otherObj);
   }

}
