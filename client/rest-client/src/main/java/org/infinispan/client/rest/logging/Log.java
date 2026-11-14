package org.infinispan.client.rest.logging;

import static org.infinispan.commons.logging.Log.LOG_ROOT;
import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.WARN;

import java.lang.invoke.MethodHandles;
import java.util.List;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;
import org.jboss.logging.annotations.ValidIdRanges;

/**
 * Logging for the REST client.
 * <p>
 * Messages at INFO level and above are internationalized; DEBUG and TRACE messages are plain strings.
 *
 * @since 16.4
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRanges({
      @ValidIdRange(min = 9001, max = 10000)
})
public interface Log extends BasicLogger {
   Log REST = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, LOG_ROOT + "REST");

   @LogMessage(level = WARN)
   @Message(value = "Failed to close the REST client", id = 9001)
   void clientCloseFailure(@Cause Throwable cause);

   @LogMessage(level = ERROR)
   @Message(value = "Error connecting to %s", id = 9002)
   void errorConnecting(String baseURL, @Cause Throwable cause);

   @Message(value = "Cannot handle authentication mechanism '%s'", id = 9003)
   IllegalArgumentException unsupportedAuthenticationMechanism(String mechanism);

   @LogMessage(level = WARN)
   @Message(value = "Authentication failed for %s: the server kept answering with 401", id = 9004)
   void authenticationFailed(String uri);

   @LogMessage(level = WARN)
   @Message(value = "No supported authentication mechanism found in challenge %s", id = 9005)
   void noSupportedAuthenticationMechanism(List<String> challengeHeaders);

   @LogMessage(level = WARN)
   @Message(value = "Skipping authentication mechanism %s", id = 9006)
   void skippingMechanism(String mechanism);

   @LogMessage(level = WARN)
   @Message(value = "No username and password provided for DIGEST authentication", id = 9007)
   void digestCredentialsMissing();

   @LogMessage(level = ERROR)
   @Message(value = "Previous digest authorization failed", id = 9008)
   void previousDigestAuthorizationFailed();

   @LogMessage(level = WARN)
   @Message(value = "Localuser authentication retry limit reached", id = 9009)
   void localuserRetryLimitReached();

   @LogMessage(level = WARN)
   @Message(value = "Failed to generate a SPNEGO token", id = 9010)
   void spnegoTokenFailure(@Cause Throwable cause);

   @LogMessage(level = ERROR)
   @Message(value = "Error in server-sent events listener for %s", id = 9011)
   void sseListenerError(String path, @Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(value = "Server returned status %d for server-sent events stream %s", id = 9012)
   void sseNonSuccessStatus(int status, String path);


}
