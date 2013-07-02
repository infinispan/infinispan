package org.infinispan.loaders.cloud.logging;

import org.infinispan.loaders.CacheLoaderException;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import java.util.Set;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the cloud cache store. For this module, message ids
 * ranging from 7001 to 8000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = WARN)
   @Message(value = "Unable to use configured Cloud Service Location [%s].  " +
         "Available locations for Cloud Service [%s] are %s", id = 7001)
   void unableToConfigureCloudService(String loc, String cloudService, Set<?> keySet);

   @LogMessage(level = INFO)
   @Message(value = "Attempt to load the same cloud bucket (%s) ignored", id = 7002)
   void attemptToLoadSameBucketIgnored(String source);

   @LogMessage(level = WARN)
   @Message(value = "Unable to read blob at %s", id = 7003)
   void unableToReadBlob(String blobName, @Cause CacheLoaderException e);

   @LogMessage(level = WARN)
   @Message(value = "Problems purging", id = 7004)
   void problemsPurging(@Cause Exception e);

}
