package org.infinispan.persistence.remote.logging;

import static org.infinispan.util.logging.Log.LOG_ROOT;

import java.lang.invoke.MethodHandles;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Log abstraction for the remote cache store.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 10001, max = 11000)
public interface Log extends BasicLogger {
   Log CONFIG = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, LOG_ROOT + "CONFIG");

   static Log getLog(Class<?> clazz) {
      return Logger.getMessageLogger(MethodHandles.lookup(), Log.class, clazz.getName());
   }

   @Message(value = "Cannot enable HotRod wrapping if a marshaller and/or an entryWrapper have already been set", id = 10005)
   CacheConfigurationException cannotEnableHotRodWrapping();

//   @Message(value = "The RemoteCacheStore for cache %s should be configured with hotRodWrapping enabled", id = 10007)
//   CacheException remoteStoreNoHotRodWrapping(String cacheName);

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

   @Message(value = "The RemoteCacheStore must specify either an existing remote-cache-container or provide a URI/server list", id = 10013)
   CacheConfigurationException remoteStoreWithoutContainer();

   @Message(value = "All stores referencing the same manager must use the same marshaller, actual is %s but provided was %s", id = 10014)
   CacheConfigurationException shouldUseSameMarshallerWithContainer(Marshaller inUse, Marshaller provided);

   @Message(value = "The remote cache container with name '%s' was not found", id = 10015)
   CacheConfigurationException unknownRemoteCacheManagerContainer(String name);

   @Message(value = "Could not migrate data for cache %s, check remote store config in the target cluster. Make sure only one remote store is present and is pointing to the source cluster", id = 10016)
   CacheException couldNotMigrateData(String name);
}
