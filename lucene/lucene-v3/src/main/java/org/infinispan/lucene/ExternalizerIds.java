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
@SuppressWarnings("boxing")
public interface ExternalizerIds {

   /**
    * @see org.infinispan.lucene.FileListCacheKey.Externalizer
    */
   static final Integer FILE_LIST_CACHE_KEY = 1300;

   /**
    * @see org.infinispan.lucene.FileMetadata.Externalizer
    */
   static final Integer FILE_METADATA = 1301;

   /**
    * @see org.infinispan.lucene.FileCacheKey.Externalizer
    */
   static final Integer FILE_CACHE_KEY = 1302;

   /**
    * @see org.infinispan.lucene.ChunkCacheKey.Externalizer
    */
   static final Integer CHUNK_CACHE_KEY = 1303;

   /**
    * @see org.infinispan.lucene.FileReadLockKey.Externalizer
    */
   static final Integer FILE_READLOCK_KEY = 1304;

}
