package org.infinispan.lucene;

/**
 * Identifiers used by the Marshaller to delegate to specialized Externalizers.
 * For details, read http://infinispan.org/docs/9.0.x/user_guide/user_guide.html#_preassigned_externalizer_id_ranges
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
   Integer FILE_LIST_CACHE_KEY = 1300;

   /**
    * @see org.infinispan.lucene.FileMetadata.Externalizer
    */
   Integer FILE_METADATA = 1301;

   /**
    * @see org.infinispan.lucene.FileCacheKey.Externalizer
    */
   Integer FILE_CACHE_KEY = 1302;

   /**
    * @see org.infinispan.lucene.ChunkCacheKey.Externalizer
    */
   Integer CHUNK_CACHE_KEY = 1303;

   /**
    * @see org.infinispan.lucene.FileReadLockKey.Externalizer
    */
   Integer FILE_READLOCK_KEY = 1304;

   /**
    * @see org.infinispan.lucene.FileListCacheKey.Externalizer
    */
   Integer FILE_LIST_CACHE_VALUE = 1305;

   /**
    * @see org.infinispan.lucene.impl.FileListCacheValueDelta.Externalizer
    */
   Integer FILE_LIST_VALUE_DELTA = 1306;

   /**
    * @see org.infinispan.lucene.impl.AddOperation.AddOperationExternalizer
    */
   Integer FILE_LIST_DELTA_ADD = 1307;

   /**
    * @see org.infinispan.lucene.impl.DeleteOperation
    */
   Integer FILE_LIST_DELTA_DEL = 1308;
}
