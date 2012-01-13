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
package org.infinispan.lucene.readlocks;

import org.infinispan.lucene.InfinispanDirectory;

/**
 * <p>SegmentReadLocker implementations have to make sure that segments are not deleted while they are
 * being used by an IndexReader.</p>
 * <p>When an {@link org.infinispan.lucene.InfinispanIndexInput} is opened on a file which is split in smaller chunks,
 * {@link #acquireReadLock(String)} is invoked; then the {@link #deleteOrReleaseReadLock(String)} is
 * invoked when the stream is closed.</p>
 * <p>The same {@link #deleteOrReleaseReadLock(String)} is invoked when a file is deleted, so if this invocation is not balancing
 * a lock acquire this implementation must delete all segment chunks and the associated metadata.</p>
 * <p>Note that if you can use and tune the {@link org.apache.lucene.index.LogByteSizeMergePolicy} you could avoid the need
 * for readlocks by setting a maximum segment size to equal the chunk size used by the InfinispanDirectory; readlocks
 * will be skipped automatically when not needed, so it's advisable to still configure an appropriate SegmentReadLocker
 * for the cases you might want to tune the chunk size.</p>
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
public interface SegmentReadLocker {

   /**
    * It will release a previously acquired readLock, or
    * if no readLock was acquired it will mark the file to be deleted as soon
    * as all pending locks are releases.
    * If it's invoked on a file without pending locks the file is deleted.
    * 
    * @param fileName of the file to release or delete
    * @see InfinispanDirectory#deleteFile(String)
    */
   void deleteOrReleaseReadLock(String fileName);

   /**
    * Acquires a readlock, in order to prevent other invocations to {@link #deleteOrReleaseReadLock(String)}
    * from deleting the file.
    * 
    * @param filename
    * @return true if the lock was acquired, false if the implementation
    * detects the file does not exist, or that it's being deleted by some other thread.
    * @see InfinispanDirectory#openInput(String)
    */
   boolean acquireReadLock(String filename);

}
