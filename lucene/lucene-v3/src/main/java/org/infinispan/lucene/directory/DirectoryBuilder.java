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
import org.infinispan.Cache;
import org.infinispan.lucene.impl.DirectoryBuilderImpl;

/**
 * Builder class to create instances of the {@link Directory} implementation which stored data
 * in the data grid.
 */
public final class DirectoryBuilder {

    private DirectoryBuilder() {
        //not to be created
    }

    /**
     * Starting point to create a Directory instance.
     * 
     * @param metadataCache contains the metadata of stored elements
     * @param chunksCache cache containing the bulk of the index; this is the larger part of data
     * @param distLocksCache cache to store locks; should be replicated and not using a persistent CacheStore
     * @param indexName identifies the index; you can store different indexes in the same set of caches using different identifiers
     */
    public static BuildContext newDirectoryInstance(Cache<?, ?> metadataCache, Cache<?, ?> chunksCache, Cache<?, ?> distLocksCache, String indexName) {
        return new DirectoryBuilderImpl(metadataCache, chunksCache, distLocksCache, indexName);
    }

}
