package org.infinispan.server.insights.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the Insights server module. For this module, message ids
 * ranging from 32001 to 32500 inclusively have been reserved.
 *
 * @author Fabio Massimo Ercoli
 * @since 14.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

   @LogMessage(level = WARN)
   @Message(value = "Invalid value for `%s` property: `%s`. Using the default: 'local'.", id = 32001)
   void insightsActivationNotValidValue(String propertyName, String invalidValue);

   @LogMessage(level = INFO)
   @Message(value = "Red Hat Insights integration is fully disabled", id = 32002)
   void insightsDisabled();

   @LogMessage(level = ERROR)
   @Message(value = "Infinispan failed to lookup the server management component. " +
         "Thus Red Hat Insights integration will be fully disabled.", id = 32003)
   void serverManagementLookupFailed();

   @LogMessage(level = INFO)
   @Message(value = "Local Insights reporting", id = 32004)
   void insightsLocallyEnabled();

   @LogMessage(level = INFO)
   @Message(value = "Remote Insights reporting", id = 32005)
   void insightsEnabled();

   @Message(value = "Error configuring Red Hat Insight client", id = 32006)
   CacheConfigurationException insightsConfigurationError();

   @LogMessage(level = ERROR)
   @Message(value = "Error getting certificate to connect to Red Hat Insight", id = 32007)
   void insightsCertificateError();

   @Message(value = "Error setting up Red Hat Insight report service", id = 32008)
   CacheException insightsServiceSetupError(@Cause Throwable t);

   @Message(value = "Red Hat Insight client shut down, it is not possible to schedule other tasks on it", id = 32009)
   CacheConfigurationException clientSchedulerShutDown();

}
