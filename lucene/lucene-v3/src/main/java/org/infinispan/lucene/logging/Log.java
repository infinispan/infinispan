package org.infinispan.lucene.logging;

import java.io.IOException;

import org.infinispan.commons.CacheException;
import org.infinispan.persistence.CacheLoaderException;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the lucene directory. For this module, message ids
 * ranging from 15001 to 16000 inclusively have been reserved.
 *
 * @author Sanne Grinovero
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = ERROR)
   @Message(value = "Error in suspending transaction", id = 15001)
   void errorSuspendingTransaction(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Unable to start transaction", id = 15002)
   void unableToStartTransaction(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Unable to commit work done", id = 15003)
   void unableToCommitTransaction(@Cause Exception e);

   @Message(value = "Unexpected format of key in String form: '%s'", id = 15004)
   IllegalArgumentException keyMappperUnexpectedStringFormat(String key);

   @LogMessage(level = DEBUG)
   @Message(value = "Lucene CacheLoader is ignoring key '%s'", id = 15005)
   void cacheLoaderIgnoringKey(Object key);

   @Message(value = "The LuceneCacheLoader requires a directory; invalid path '%s'", id = 15006)
   CacheException rootDirectoryIsNotADirectory(String fileRoot);

   @Message(value = "LuceneCacheLoader was unable to create the root directory at path '%s'", id = 15007)
   CacheException unableToCreateDirectory(String fileRoot);

   @Message(value = "IOException happened in the CacheLoader", id = 15008)
   CacheLoaderException exceptionInCacheLoader(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to close FSDirectory", id = 15009)
   void errorOnFSDirectoryClose(@Cause IOException e);

   @LogMessage(level = WARN)
   @Message(value = "Error happened while looking for FSDirectories in '%s'", id = 15010)
   void couldNotWalkDirectory(String name, @Cause CacheLoaderException e);

   @LogMessage(level = WARN)
   @Message(value = "The configured autoChunkSize is too small for segment file %s as it is %d bytes; auto-scaling chunk size to %d", id = 15011)
   void rescalingChunksize(String fileName, long fileLength, int chunkSize);

   @Message(value = "Could not instantiate Directory implementation for Lucene 4 compatibility", id = 15012)
   CacheException failedToCreateLucene4Directory(@Cause Exception e);

   @LogMessage(level = DEBUG)
   @Message(value = "Lucene Directory detected Apache Lucene v. '%d' - this will affect which APIs are going to be provided", id = 15013)
   void detectedLuceneVersion(int version);

   @Message(value = "Lucene Directory for index '%s' can not use Cache '%s': maximum lifespan enabled on the Cache configuration!", id = 15014)
   IllegalArgumentException luceneStorageHavingLifespanSet(String indexName, String cacheName);

   @Message(value = "Lucene Directory for index '%s' can not use Cache '%s': expiration idle time enabled on the Cache configuration!", id = 15015)
   IllegalArgumentException luceneStorageHavingIdleTimeSet(String indexName, String cacheName);

   @Message(value = "'%s' must not be null", id = 15016)
   IllegalArgumentException requiredParameterWasPassedNull(String objectname);

   @Message(value = "Lucene Directory for index '%s' can not use Cache '%s': store as binary enabled on the Cache configuration!", id = 15017)
   IllegalArgumentException luceneStorageAsBinaryEnabled(String indexName, String cacheName);

   @Message(value = "Lucene Directory for index '%s' can not use Metadata Cache '%s': eviction enabled on the Cache configuration!", id = 15018)
   IllegalArgumentException evictionNotAllowedInMetadataCache(String indexName, String cacheName);

   @Message(value = "Lucene Directory for index '%s' can not use Metadata Cache '%s': persistence enabled without preload on the Cache configuration!", id = 15019)
   IllegalArgumentException preloadNeededIfPersistenceIsEnabledForMetadataCache(String indexName, String cacheName);
}
