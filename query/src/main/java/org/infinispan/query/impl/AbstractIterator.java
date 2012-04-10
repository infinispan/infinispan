/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.AdvancedCache;
import org.infinispan.query.QueryIterator;

/**
 * This is the abstract superclass of the 2 iterators. Since some of the methods have the same implementations they have
 * been put onto a separate class.
 *
 * @author Navin Surtani
 * @see org.infinispan.query.impl.EagerIterator
 * @see org.infinispan.query.impl.LazyIterator
 * @since 4.0
 */
public abstract class AbstractIterator implements QueryIterator {

   protected Object[] buffer;
   protected AdvancedCache<?, ?> cache;

   protected int index = 0;
   protected int bufferIndex = -1;
   protected int max;
   protected int first;
   protected int fetchSize;

   @Override
   public void first() {
      index = first;
   }

   @Override
   public void last() {
      index = max;
   }

   @Override
   public void afterFirst() {
      index = first + 1;
   }

   @Override
   public void beforeLast() {
      index = max - 1;
   }

   @Override
   public boolean isFirst() {
      return index == first;
   }

   @Override
   public boolean isLast() {
      return index == max;
   }

   @Override
   public boolean isAfterFirst() {
      return index == first + 1;
   }

   @Override
   public boolean isBeforeLast() {
      return index == max - 1;
   }

   @Override
   public boolean hasPrevious() {
      return index >= first;
   }

   @Override
   public boolean hasNext() {
      return index <= max;
   }

}
