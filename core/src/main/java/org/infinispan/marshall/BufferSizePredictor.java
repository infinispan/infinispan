/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.marshall;

/**
 * Buffer size predictor
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public interface BufferSizePredictor {

   /**
    * Provide the next buffer size taking in account
    * the object to store in the buffer.
    *
    * @param obj instance that will be stored in the buffer
    * @return int representing the next predicted buffer size
    */
   int nextSize(Object obj);

   /**
    * Record the size of the of data in the last buffer used.
    *
    * @param previousSize int representing the size of the last
    *                         object buffered.
    */
   void recordSize(int previousSize);

}
