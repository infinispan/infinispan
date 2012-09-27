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
package org.infinispan.query;

import java.util.ListIterator;

/**
 * Iterates over query results
 * <p/>
 *
 * @author Manik Surtani
 * @author Navin Surtani
 * @author Marko Luksa
 */
public interface QueryIterator extends ListIterator<Object> {
   /**
    * Jumps to a specific position in the iterator. The specified index indicates the first element that would be
    * returned by an initial call to next. An initial call to previous would return the element with the specified
    * index minus one.
    *
    * @param index index to jump to.
    * @throws IndexOutOfBoundsException if the index is out of bounds (index < 0 || index > size())
    */
   void jumpToIndex(int index) throws IndexOutOfBoundsException;

   /**
    * Jumps to the first result
    */
   void beforeFirst();

   /**
    * Jumps to the last result
    */
   void afterLast();

   /**
    * This method must be called on your iterator once you have finished so that Lucene resources can be freed up.
    */
   void close();
}
