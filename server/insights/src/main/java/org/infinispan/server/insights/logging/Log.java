package org.infinispan.server.insights.logging;

import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

import java.lang.invoke.MethodHandles;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Log abstraction for the Insights server module.
 *
 * @author Fabio Massimo Ercoli
 * @since 14.0
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 32001, max = 32500)
public interface Log extends BasicLogger {
   static Log getLog(Class<?> clazz) {
      return Logger.getMessageLogger(MethodHandles.lookup(), Log.class, clazz.getName());
   }

   @LogMessage(level = WARN)
   @Message(value = "Invalid value for `%s` property: `%s`. Using the default: 'local'.", id = 32001)
   void insightsActivationNotValidValue(String propertyName, String invalidValue);

   @LogMessage(level = INFO)
   @Message(value = "Red Hat Insights integration is fully disabled", id = 32002)
   void insightsDisabled();

   @LogMessage(level = WARN)
   @Message(value = "Infinispan failed to lookup the server management component. " +
         "Thus Red Hat Insights integration will be fully disabled.", id = 32003)
   void serverManagementLookupFailed();

   @LogMessage(level = INFO)
   @Message(value = "Local Insights reporting", id = 32004)
   void insightsLocallyEnabled();

   @LogMessage(level = INFO)
   @Message(value = "Remote Insights reporting", id = 32005)
   void insightsEnabled();

   @LogMessage(level = INFO)
   @Message(value = "The environment variable '%s' or the system property '%s' is set to true. " +
         "The Insights reports will be disabled.", id = 32006)
   void optOutTrue(String environmentVariable, String systemProperty);

   @LogMessage(level = INFO)
   @Message(value = "Error getting certificate to connect to Red Hat Insight", id = 32007)
   void insightsCertificateError();

   @LogMessage(level = INFO)
   @Message(value = "Error setting up Red Hat Insight report service", id = 32008)
   void insightsServiceSetupError(@Cause Throwable t);

   @LogMessage(level = INFO)
   @Message(value = "Red Hat Insight client shut down, it is not possible to schedule other tasks on it", id = 32009)
   void clientSchedulerShutDown();

}
