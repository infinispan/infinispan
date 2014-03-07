package org.infinispan.server.core.logging;

import io.netty.channel.Channel;
import org.infinispan.commons.CacheConfigurationException;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import java.net.SocketAddress;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the server core module. For this module, message ids
 * ranging from 5001 to 6000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface JavaLog extends org.infinispan.util.logging.Log {

   @LogMessage(level = INFO)
   @Message(value = "Start main with args: %s", id = 5001)
   void startWithArgs(String args);

   @LogMessage(level = INFO)
   @Message(value = "Posting Shutdown Request to the server...", id = 5002)
   void postingShutdownRequest();

   @LogMessage(level = ERROR)
   @Message(value = "Exception reported", id = 5003)
   void exceptionReported(@Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "Server channel group did not completely unbind", id = 5004)
   void serverDidNotUnbind();

   @LogMessage(level = WARN)
   @Message(value = "%s is still bound to %s", id = 5005)
   void channelStillBound(Channel ch, SocketAddress address);

   @LogMessage(level = WARN)
   @Message(value = "Channel group did not completely close", id = 5006)
   void serverDidNotClose();

   @LogMessage(level = WARN)
   @Message(value = "%s is still connected to %s", id = 5007)
   void channelStillConnected(Channel ch, SocketAddress address);

   @LogMessage(level = WARN)
   @Message(value = "Setting the number of master threads is no longer supported", id = 5008)
   void settingMasterThreadsNotSupported();

   @LogMessage(level = ERROR)
   @Message(value = "Unexpected error before any request parameters read", id = 5009)
   void errorBeforeReadingRequest(@Cause Throwable t);

   @Message(value = "Illegal number of workerThreads: %d", id = 5010)
   IllegalArgumentException illegalWorkerThreads(int workerThreads);

   @Message(value = "Idle timeout can't be lower than -1: %d", id = 5011)
   IllegalArgumentException illegalIdleTimeout(int idleTimeout);

   @Message(value = "Receive Buffer Size can't be lower than 0: %d", id = 5012)
   IllegalArgumentException illegalReceiveBufferSize(int recvBufSize);

   @Message(value = "Send Buffer Size can't be lower than 0: %d", id = 5013)
   IllegalArgumentException illegalSendBufferSize(int sendBufSize);

   @Message(value = "SSL Enabled but no KeyStore specified", id = 5014)
   CacheConfigurationException noSSLKeyManagerConfiguration();

   @Message(value = "SSL Enabled but no TrustStore specified", id = 5015)
   CacheConfigurationException noSSLTrustManagerConfiguration();

   @Message(value = "A password is required to open the KeyStore '%s'", id = 5016)
   CacheConfigurationException missingKeyStorePassword(String keyStore);

   @Message(value = "A password is required to open the TrustStore '%s'", id = 5017)
   CacheConfigurationException missingTrustStorePassword(String trustStore);

   @Message(value = "Cannot configure custom KeyStore and/or TrustStore when specifying a SSLContext", id = 5018)
   CacheConfigurationException xorSSLContext();
}
