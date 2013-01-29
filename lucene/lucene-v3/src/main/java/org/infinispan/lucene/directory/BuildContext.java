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
package org.infinispan.lucene.directory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.infinispan.lucene.readlocks.SegmentReadLocker;

/**
 * Building context to set construction parameters of Infinispan Directory instances
 *
 * @since 5.2
 * @author Sanne Grinovero
 */
public interface BuildContext {

   /**
    * Creates a Directory instance
    * @see org.apache.lucene.store.Directory
    * @return the new Directory
    */
   Directory create();

   /**
    * Sets the chunkSize option for the Directory being created.
    * 
    * @param bytes segments are fragmented in chunkSize bytes; larger values are more efficient for searching but less for
    *        distribution and network replication
    * @return the same building context to eventually create the Directory instance
    */
   BuildContext chunkSize(int bytes);

   /**
    * Overrides the default SegmentReadLocker. In some cases you might be able to provide more efficient implementations than
    * the default one by controlling the IndexReader's lifecycle
    * 
    * @see org.infinispan.lucene.readlocks
    * @param srl the new read locking strategy for fragmented segments
    * @return the same building context to eventually create the Directory instance
    */
   BuildContext overrideSegmentReadLocker(SegmentReadLocker srl);

   /**
    * Overrides the IndexWriter LockFactory
    * 
    * @see org.infinispan.lucene.locking
    * @param lf the LockFactory to be used by IndexWriters.
    * @return the same building context to eventually create the Directory instance
    */
   BuildContext overrideWriteLocker(LockFactory lf);

}
