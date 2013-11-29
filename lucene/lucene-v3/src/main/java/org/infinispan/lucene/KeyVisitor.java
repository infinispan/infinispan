package org.infinispan.lucene;

/**
 * All objects being used as keys to store entries by the Lucene Directory
 * implement {@link IndexScopedKey} which enforces visitability with a
 * {@link KeyVisitor<T>}. Various components use this to visit each key.
 *
 * @author Sanne Grinovero
 * @since 5.2
 */
public interface KeyVisitor<T> {

   T visit(FileListCacheKey fileListCacheKey) throws Exception;

   T visit(ChunkCacheKey chunkCacheKey) throws Exception;

   T visit(FileCacheKey fileCacheKey) throws Exception;

   T visit(FileReadLockKey fileReadLockKey) throws Exception;

}
