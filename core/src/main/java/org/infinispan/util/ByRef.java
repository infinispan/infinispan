/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.util;

/**
 * This class can be used to pass an argument by reference.
 * @param <T> The wrapped type.
 *
 * @author Dan Berindei
 * @since 5.1
 */
public class ByRef<T> {
   private T ref;

   public ByRef(T t) {
      ref = t;
   }

   public static <T> ByRef<T> create(T t) {
      return new ByRef<T>(t);
   }

   public T get() {
      return ref;
   }

   public void set(T t) {
      ref = t;
   }
}
