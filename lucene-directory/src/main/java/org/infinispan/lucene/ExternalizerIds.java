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
package org.infinispan.lucene;

/**
 * Identifiers used by the Marshaller to delegate to specialized Externalizers.
 * For details, read http://community.jboss.org/docs/DOC-16198
 * 
 * The range reserved for the Lucene module is from 1300 to 1399.
 * 
 * @author Sanne Grinovero
 * @since 5.0
 */
public interface ExternalizerIds {

   /**
    * @see org.infinispan.lucene.FileListCacheKey.Externalizer
    */
   static final int FILE_LIST_CACHE_KEY = 1300;
   
   /**
    * @see org.infinispan.lucene.FileMetadata.Externalizer
    */
   static final int FILE_METADATA = 1301;
   
   /**
    * @see org.infinispan.lucene.FileCacheKey.Externalizer
    */
   static final int FILE_CACHE_KEY = 1302;
   
   /**
    * @see org.infinispan.lucene.ChunkCacheKey.Externalizer
    */
   static final int CHUNK_CACHE_KEY = 1303;
   
   /**
    * @see org.infinispan.lucene.FileReadLockKey.Externalizer
    */
   static final int FILE_READLOCK_KEY = 1304;

}
