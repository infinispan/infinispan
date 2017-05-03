package org.infinispan.client.hotrod.logging;

import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.event.IncorrectClientListenerException;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransport;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.CacheListenerException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the hot rod client. For this module, message ids
 * ranging from 4001 to 5000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

   @LogMessage(level = WARN)
   @Message(value = "Could not find '%s' file in classpath, using defaults.", id = 4001)
   void couldNotFindPropertiesFile(String propertiesFile);

   @LogMessage(level = INFO)
   @Message(value = "Cannot perform operations on a cache associated with an unstarted RemoteCacheManager. Use RemoteCacheManager.start before using the remote cache.", id = 4002)
   void unstartedRemoteCacheManager();

   @LogMessage(level = ERROR)
   @Message(value = "Invalid magic number. Expected %#x and received %#x", id = 4003)
   void invalidMagicNumber(short expectedMagicNumber, short receivedMagic);

   @LogMessage(level = ERROR)
   @Message(value = "Invalid message id. Expected %d and received %d", id = 4004)
   void invalidMessageId(long expectedMsgId, long receivedMsgId);

   @LogMessage(level = WARN)
   @Message(value = "Error received from the server: %s", id = 4005)
   void errorFromServer(String message);

   @LogMessage(level = INFO)
   @Message(value = "%s sent new topology view (id=%d, age=%d) containing %d addresses: %s", id = 4006)
   void newTopology(SocketAddress address, int viewId, int age, int topologySize, Set<SocketAddress> topology);

   @LogMessage(level = ERROR)
   @Message(value = "Exception encountered. Retry %d out of %d", id = 4007)
   void exceptionAndNoRetriesLeft(int retry, int maxRetries, @Cause HotRodClientException te);

   //  id = 4008 is now logged to TRACE(ISPN-1794)

   @LogMessage(level = WARN)
   @Message(value = "Issues closing socket for %s: %s", id = 4009)
   void errorClosingSocket(TcpTransport transport, IOException e);

   @LogMessage(level = WARN)
   @Message(value = "Exception while shutting down the connection pool.", id = 4010)
   void errorClosingConnectionPool(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "No hash function configured for version: %d", id = 4011)
   void noHasHFunctionConfigured(int hashFunctionVersion);

   @LogMessage(level = WARN)
   @Message(value = "Could not invalidate connection: %s", id = 4012)
   void couldNoInvalidateConnection(TcpTransport transport, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Could not release connection: %s", id = 4013)
   void couldNotReleaseConnection(TcpTransport transport, @Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "New server added(%s), adding to the pool.", id = 4014)
   void newServerAdded(SocketAddress server);

   @LogMessage(level = WARN)
   @Message(value = "Failed adding new server %s", id = 4015)
   void failedAddingNewServer(SocketAddress server, @Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "Server not in cluster anymore(%s), removing from the pool.", id = 4016)
   void removingServer(SocketAddress server);

   @LogMessage(level = WARN)
   @Message(value = "Unable to convert string property [%s] to an int! Using default value of %d", id = 4018)
   void unableToConvertStringPropertyToInt(String value, int defaultValue);

   @LogMessage(level = WARN)
   @Message(value = "Unable to convert string property [%s] to a long! Using default value of %d", id = 4019)
   void unableToConvertStringPropertyToLong(String value, long defaultValue);

   @LogMessage(level = WARN)
   @Message(value = "Unable to convert string property [%s] to a boolean! Using default value of %b", id = 4020)
   void unableToConvertStringPropertyToBoolean(String value, boolean defaultValue);

   @LogMessage(level = INFO)
   @Message(value = "Infinispan version: %s", id = 4021)
   void version(String version);

   @LogMessage(level = WARN)
   @Message(value = "Unable to invalidate transport for server: %s", id = 4022)
   void unableToInvalidateTransport(SocketAddress serverAddress);

   @Message(value = "SSL Enabled but no KeyStore specified", id = 4023)
   CacheConfigurationException noSSLKeyManagerConfiguration();

   @Message(value = "SSL Enabled but no TrustStore specified", id = 4024)
   CacheConfigurationException noSSLTrustManagerConfiguration();

   @Message(value = "A password is required to open the KeyStore '%s'", id = 4025)
   CacheConfigurationException missingKeyStorePassword(String keyStore);

   @Message(value = "A password is required to open the TrustStore '%s'", id = 4026)
   CacheConfigurationException missingTrustStorePassword(String trustStore);

   @Message(value = "Cannot configure custom KeyStore and/or TrustStore when specifying a SSLContext", id = 4027)
   CacheConfigurationException xorSSLContext();

   @Message(value = "Unable to parse server IP address %s", id = 4028)
   CacheConfigurationException parseErrorServerAddress(String server);

   @Message(value = "Invalid max_retries (value=%s). Value should be greater or equal than zero.", id = 4029)
   CacheConfigurationException invalidMaxRetries(int retriesPerServer);

   @Message(value = "Cannot enable authentication without specifying a Callback Handler or a client Subject", id = 4030)
   CacheConfigurationException invalidCallbackHandler();

   @Message(value = "The selected authentication mechanism '%s' is not among the supported server mechanisms: %s", id = 4031)
   SecurityException unsupportedMech(String authMech, List<String> serverMechs);

   @Message(value = "'%s' is an invalid SASL mechanism", id = 4032)
   CacheConfigurationException invalidSaslMechanism(String saslMechanism);

   @Message(value = "Connection dedicated to listener with id=%s but received event for listener with id=%s", id = 4033)
   IllegalStateException unexpectedListenerId(String listenerId, String expectedListenerId);

   @Message(value = "Unable to unmarshall bytes %s", id = 4034)
   HotRodClientException unableToUnmarshallBytes(String bytes, @Cause Exception e);

   @Message(value = "Caught exception [%s] while invoking method [%s] on listener instance: %s", id = 4035)
   CacheListenerException exceptionInvokingListener(String name, Method m, Object target, @Cause Throwable cause);

   @Message(value = "Methods annotated with %s must accept exactly one parameter, of assignable from type %s", id = 4036)
   IncorrectClientListenerException incorrectClientListener(String annotationName, Collection<?> allowedParameters);

   @Message(value = "Methods annotated with %s should have a return type of void.", id = 4037)
   IncorrectClientListenerException incorrectClientListener(String annotationName);

   @LogMessage(level = ERROR)
   @Message(value = "Unexpected error consuming event %s", id = 4038)
   void unexpectedErrorConsumingEvent(ClientEvent clientEvent, @Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "Unable to complete reading event from server %s", id = 4039)
   void unableToReadEventFromServer(@Cause Throwable t, SocketAddress server);

   @Message(value = "Cache listener class %s must be annotated with org.infinispan.client.hotrod.annotation.ClientListener", id = 4040)
   IncorrectClientListenerException missingClientListenerAnnotation(String className);

   @Message(value = "Unknown event type %s received", id = 4041)
   HotRodClientException unknownEvent(short eventTypeId);

   @LogMessage(level = ERROR)
   @Message(value = "Unable to set method %s accessible", id = 4042)
   void unableToSetAccesible(Method m, @Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Unrecoverable error reading event from server %s, exiting event reader thread", id = 4043)
   void unrecoverableErrorReadingEvent(@Cause Throwable t, SocketAddress server);

   @LogMessage(level = ERROR)
   @Message(value = "Unable to read %s bytes %s", id = 4044)
   void unableToUnmarshallBytesError(String element, String bytes, @Cause Exception e);

   @Message(value = "When enabling near caching, number of max entries must be configured", id = 4045)
   CacheConfigurationException nearCacheMaxEntriesUndefined();

   @LogMessage(level = INFO)
   @Message(value = "Successfully closed remote iterator '%s'", id = 4046)
   void iterationClosed(String iterationId);

   @Message(value = "Invalid iteration id '%s'", id = 4047)
   IllegalStateException errorClosingIteration(String iterationId);

   @Message(value = "Invalid iteration id '%s'", id = 4048)
   NoSuchElementException errorRetrievingNext(String iterationId);

   @LogMessage(level = WARN)
   @Message(value = "No consistent hash is available in the client, starting iteration using the configured request balancing strategy", id = 4049)
   void noConsistentHashAvailable();

   @LogMessage(level = INFO)
   @Message(value = "Switched to cluster '%s'", id = 4050)
   void switchedToCluster(String clusterName);

   @LogMessage(level = INFO)
   @Message(value = "Switched back to main cluster", id = 4051)
   void switchedBackToMainCluster();

   @LogMessage(level = INFO)
   @Message(value = "Manually switched to cluster '%s'", id = 4052)
   void manuallySwitchedToCluster(String clusterName);

   @LogMessage(level = INFO)
   @Message(value = "Manually switched back to main cluster", id = 4053)
   void manuallySwitchedBackToMainCluster();

   @Message(value = "Name of the failover cluster needs to be specified", id = 4054)
   CacheConfigurationException missingClusterNameDefinition();

   @Message(value = "Host needs to be specified in server definition of failover cluster", id = 4055)
   CacheConfigurationException missingHostDefinition();

   @Message(value = "At least one server address needs to be specified for failover cluster %s", id = 4056)
   CacheConfigurationException missingClusterServersDefinition(String siteName);

   @Message(value = "Duplicate failover cluster %s has been specified", id = 4057)
   CacheConfigurationException duplicateClusterDefinition(String siteName);

   @Message(value = "The client listener must use raw data when it uses a query as a filter: %s", id = 4058)
   IncorrectClientListenerException clientListenerMustUseRawData(String className);

   @Message(value = "The client listener must use the '%s' filter/converter factory", id = 4059)
   IncorrectClientListenerException clientListenerMustUseDesignatedFilterConverterFactory(String filterConverterFactoryName);

   @Message(value = "Query parameter '%s' was not set", id = 4060)
   IllegalStateException queryParameterNotSet(String filterConverterFactoryName);

   @LogMessage(level = WARN)
   @Message(value = "Ignoring error when closing iteration '%s'", id = 4061)
   void ignoringErrorDuringIterationClose(String iterationId, @Cause Exception e);

   @LogMessage(level = DEBUG)
   @Message(value = "Started iteration '%s'", id = 4062)
   void startedIteration(String iterationId);

   @LogMessage(level = DEBUG)
   @Message(value = "Transport '%s' obtained for iteration '%s'", id = 4063)
   void iterationTransportObtained(Transport transport, String iterationId);

   @LogMessage(level = TRACE)
   @Message(value = "Tracking key %s belonging to segment %d, already tracked? = %b", id = 4064)
   void trackingSegmentKey(String key, int segment, boolean isTracked);

   @LogMessage(level = WARN)
   @Message(value = "Classpath does not look correct. Make sure you are not mixing uber and jars", id = 4065)
   void warnAboutUberJarDuplicates();

   @Message(value = "Class '%s' blocked by Java standard deserialization white list. Adjust the client configuration java serialization white list regular expression to include this class.", id = 4068)
   CacheException classNotInWhitelist(String className);

}
