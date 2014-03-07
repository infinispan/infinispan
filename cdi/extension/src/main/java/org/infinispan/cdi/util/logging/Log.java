package org.infinispan.cdi.util.logging;

import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import static org.jboss.logging.Logger.Level.INFO;

/**
 * The JBoss Logging interface which defined the logging methods for the CDI integration. The id range for the CDI
 * integration is 17001-18000
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

   @LogMessage(level = INFO)
   @Message(value = "Infinispan CDI extension version: %s", id = 17001)
   void version(String version);

   @LogMessage(level = INFO)
   @Message(value = "Configuration for cache '%s' has been defined in cache manager '%s'", id = 17002)
   void cacheConfigurationDefined(String cacheName, EmbeddedCacheManager cacheManager);

   @Message(value = "%s parameter must not be null", id = 17003)
   IllegalArgumentException parameterMustNotBeNull(String parameterName);

}
