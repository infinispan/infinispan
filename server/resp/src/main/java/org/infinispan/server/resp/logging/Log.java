package org.infinispan.server.resp.logging;

import org.infinispan.commons.CacheConfigurationException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the Resp protocol server module. For this module, message ids
 * ranging from 12001 to 13000 inclusively have been reserved.
 *
 * @author William Burns
 * @since 14.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   @Message(value = "Cache '%s' has expiration enabled which violates the Resp protocol", id = 12001)
   CacheConfigurationException invalidExpiration(String cacheName);

   @Message(value = "Cannot enable authentication without an authenticator", id = 12002)
   CacheConfigurationException authenticationWithoutAuthenticator();
}
