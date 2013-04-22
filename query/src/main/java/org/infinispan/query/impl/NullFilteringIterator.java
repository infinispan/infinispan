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
package org.infinispan.query.impl;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator wrapper that filters out (skips over) any null values returned by the wrapped iterator.
 *
 * @author Marko Luksa
 */
public class NullFilteringIterator<E> implements Iterator<E> {

   private Iterator<E> delegate;
   private E next;

   public NullFilteringIterator(Iterator<E> delegate) {
      this.delegate = delegate;
   }

   @Override
   public boolean hasNext() {
      if (next != null) {
         return true;
      }

      while (delegate.hasNext()) {
         next = delegate.next();
         if (next != null) {
            return true;
         }
      }
      return false;
   }

   @Override
   public E next() {
      if (!hasNext()) {
         throw new NoSuchElementException();
      }
      try {
         return next;
      } finally {
         next = null;
      }
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException("remove() is not supported");
   }
}
