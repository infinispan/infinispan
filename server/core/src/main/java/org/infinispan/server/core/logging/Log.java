package org.infinispan.server.core.logging;

import static org.jboss.logging.Logger.Level.DEBUG;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.util.Set;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.core.dataconversion.TranscodingException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import io.netty.channel.Channel;
import io.netty.handler.ipfilter.IpFilterRule;

/**
 * Log abstraction for the server core module. For this module, message ids
 * ranging from 5001 to 6000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   String LOG_ROOT = "org.infinispan.";
   Log CONFIG = Logger.getMessageLogger(Log.class, LOG_ROOT + "CONFIG");
   Log SECURITY = Logger.getMessageLogger(Log.class, LOG_ROOT + "SECURITY");
   Log SERVER = Logger.getMessageLogger(Log.class, LOG_ROOT + "SERVER");
//   @LogMessage(level = WARN)
//   @Message(value = "Server channel group did not completely unbind", id = 5004)
//   void serverDidNotUnbind();

   @LogMessage(level = WARN)
   @Message(value = "%s is still bound to %s", id = 5005)
   void channelStillBound(Channel ch, SocketAddress address);

//   @LogMessage(level = WARN)
//   @Message(value = "Channel group did not completely close", id = 5006)
//   void serverDidNotClose();

   @LogMessage(level = WARN)
   @Message(value = "%s is still connected to %s", id = 5007)
   void channelStillConnected(Channel ch, SocketAddress address);

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

   @Message(value = "A password is required to open the KeyStore '%s'", id = 5016)
   CacheConfigurationException missingKeyStorePassword(String keyStore);

   @Message(value = "A password is required to open the TrustStore '%s'", id = 5017)
   CacheConfigurationException missingTrustStorePassword(String trustStore);

   @Message(value = "Cannot configure custom KeyStore and/or TrustStore when specifying a SSLContext", id = 5018)
   CacheConfigurationException xorSSLContext();

   @LogMessage(level = DEBUG)
   @Message(value = "Using Netty SocketChannel %s for %s", id = 5025)
   void createdSocketChannel(String channelClassName, String configuration);

   @LogMessage(level = DEBUG)
   @Message(value = "Using Netty EventLoop %s for %s", id = 5026)
   void createdNettyEventLoop(String eventLoopClassName, String configuration);

   @Message(value = "SSL Enabled but no SNI domain configured", id = 5027)
   CacheConfigurationException noSniDomainConfigured();

   @LogMessage(level = INFO)
   @Message(value = "Native Epoll transport not available, using NIO instead: %s", id = 5028)
   void epollNotAvailable(String message);

   @Message(value = "No task manager available to register the admin operations handler", id = 5029)
   CacheConfigurationException cannotRegisterAdminOperationsHandler();

   @Message(value = "Administration task '%s' invoked without required parameter '%s'", id = 5030)
   NullPointerException missingRequiredAdminTaskParameter(String name, String parameter);

   @Message(value = "The supplied configuration for cache '%s' is missing a named configuration for it: %s", id = 5031)
   CacheConfigurationException missingCacheConfiguration(String name, String configuration);

//   @Message(value = "Error during transcoding", id = 5032)
//   TranscodingException errorDuringTranscoding(@Cause Throwable e);

   @Message(value = "Data format '%s' not supported", id = 5033)
   TranscodingException unsupportedDataFormat(MediaType contentFormat);

   @Message(value = "Cannot create clustered caches in non-clustered servers", id = 5034)
   UnsupportedOperationException cannotCreateClusteredCache();

   @Message(value = "Class '%s' blocked by deserialization allow list. Include the class name in the server cache manager allow list to authorize.", id = 5035)
   CacheException errorDeserializing(String className);

   @Message(value = "Illegal number of ioThreads: %d", id = 5036)
   IllegalArgumentException illegalIOThreads(int ioThreads);

//   @Message(value = "No provider for authorization realm", id = 5037)
//   XMLStreamException noProviderForAuthorizationRealm();

   @Message(value = "Illegal type for parameter '%s': %s", id = 5038)
   IllegalArgumentException illegalParameterType(String parameter, Class<?> type);

   @Message(value = "Cannot create cluster backup", id = 5039)
   CacheException errorCreatingBackup(@Cause Throwable cause);

   @Message(value = "Cannot restore cluster backup '%s'", id = 5040)
   CacheException errorRestoringBackup(Path path, @Cause Throwable cause);

   @Message(value = "Cannot perform backup, backup currently in progress", id = 5041)
   CacheException backupInProgress();

   @Message(value = "Cannot restore content, restore currently in progress", id = 5042)
   CacheException restoreInProgress();

   @LogMessage(level = INFO)
   @Message(value = "Starting backup '%s'", id = 5043)
   void initiatingBackup(String name);

   @LogMessage(level = INFO)
   @Message(value = "Backup file created '%s'", id = 5044)
   void backupComplete(String backupName);

   @LogMessage(level = INFO)
   @Message(value = "Starting restore '%s' of '%s'", id = 5045)
   void initiatingRestore(String name, Path backup);

   @LogMessage(level = INFO)
   @Message(value = "Restore '%s' complete", id = 5046)
   void restoreComplete(String name);

   @Message(value = "%s '%s' not found in the backup archive", id = 5047)
   CacheException unableToFindBackupResource(String resource, Set<String> resourceNames);

   @Message(value = "%s '%s' does not exist", id = 5048)
   CacheException unableToFindResource(String resource, String resourceName);

   @Message(value = "Cannot perform backup, backup already exists with name '%s'", id = 5049)
   CacheException backupAlreadyExists(String name);

   @LogMessage(level = INFO)
   @Message(value = "Deleted backup '%s'", id = 5050)
   void backupDeleted(String name);

   @Message(value = "Cannot perform restore, restore already exists with name '%s'", id = 5051)
   CacheException restoreAlreadyExists(String name);

   @LogMessage(level = INFO)
   @Message(value = "Rejected connection from '%s' using rule '%s'", id = 5052)
   void ipFilterConnectionRejection(InetSocketAddress remoteAddress, IpFilterRule rule);

   @Message(value = "The supplied configuration for cache '%s' must contain a single cache configuration for it: %s", id = 5053)
   CacheConfigurationException configurationMustContainSingleCache(String name, String configuration);

   @LogMessage(level = INFO)
   @Message(value = "Native IOUring transport not available, using NIO instead: %s", id = 5054)
   void ioUringNotAvailable(String message);

   @LogMessage(level = INFO)
   @Message(value = "Using transport: %s", id = 5055)
   void usingTransport(String transportName);

   @Message(value = "Cannot enable authentication without specifying a SaslAuthenticationProvider", id = 5056)
   CacheConfigurationException saslAuthenticationProvider();

   @Message(value = "The specified allowedMechs [%s] contains mechs which are unsupported by the underlying factories [%s]", id = 5057)
   CacheConfigurationException invalidAllowedMechs(Set<String> allowedMechs, Set<String> allMechs);

   @Message(value = "A serverName must be specified when enabling authentication", id = 5058)
   CacheConfigurationException missingServerName();

   @Message(value = "EXTERNAL SASL mechanism not allowed without SSL client certificate", id = 5059)
   SecurityException externalMechNotAllowedWithoutSSLClientCert();

   // Out-of-order log messages. Moved here from the server-hotrod module
   @Message(value = "Factory '%s' not found in server", id = 6016)
   IllegalStateException missingKeyValueFilterConverterFactory(String name);

   @LogMessage(level = WARN)
   @Message(value = "Removed unclosed iterator '%s'", id = 28026)
   void removedUnclosedIterator(String iteratorId);

   @LogMessage(level = INFO)
   @Message(value = "Flushed cache for security realm '%s'", id = 28027)
   void flushRealmCache(String name);
}
