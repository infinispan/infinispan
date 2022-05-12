package org.infinispan.persistence.remote.logging;

import static org.infinispan.util.logging.Log.LOG_ROOT;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the remote cache store. For this module, message ids
 * ranging from 10001 to 11000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   Log CONFIG = Logger.getMessageLogger(Log.class, LOG_ROOT + "CONFIG");

   @Message(value = "Could not find migration data in cache %s", id = 276)
   CacheException missingMigrationData(String name);

   @LogMessage(level = WARN)
   @Message(value = "Could not migrate key %s", id = 277)
   void keyMigrationFailed(String key, @Cause Throwable cause);

   @LogMessage(level = INFO)
   @Message(value = "Ignoring XML attribute %s, please remove from configuration file", id = 293)
   void ignoreXmlAttribute(Object attribute);

   @Message(value = "Could not migrate data for cache %s, check remote store config in the target cluster. Make sure only one remote store is present and is pointing to the source cluster", id = 397)
   CacheException couldNotMigrateData(String name);

   @Message(value = "Cannot enable HotRod wrapping if a marshaller and/or an entryWrapper have already been set", id = 10005)
   CacheConfigurationException cannotEnableHotRodWrapping();

   @Message(value = "The RemoteCacheStore for cache %s should be configured with hotRodWrapping enabled", id = 10007)
   CacheException remoteStoreNoHotRodWrapping(String cacheName);

   @Message(value = "RemoteStore only supports segmentation when using at least protocol version %s or higher", id = 10008)
   CacheConfigurationException segmentationNotSupportedInThisVersion(ProtocolVersion version);

   @Message(value = "A RemoteStore must be shared in a cache that is clustered", id = 10009)
   CacheConfigurationException clusteredRequiresBeingShared();

   @Message(value = "Segmentation is not supported for a RemoteStore when the configured segments %d do not match the remote servers amount %s", id = 10010)
   CacheConfigurationException segmentationRequiresEqualSegments(int cacheSegmentCount, Integer serverSegmentCount);

   @Message(value = "Segmentation is not supported for a RemoteStore when the configured key media type %s does not match the remote servers key media type %s", id = 10011)
   CacheConfigurationException segmentationRequiresEqualMediaTypes(MediaType cacheMediaType, MediaType serverMediaType);

   @Message(value = "The RemoteCacheStore cannot be segmented when grouping is enabled", id = 10012)
   CacheConfigurationException segmentationNotSupportedWithGroups();
}
