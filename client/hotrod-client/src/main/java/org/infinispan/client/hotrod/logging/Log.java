package org.infinispan.client.hotrod.logging;

import static org.jboss.logging.Logger.Level.DEBUG;
import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.TRACE;
import static org.jboss.logging.Logger.Level.WARN;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.event.IncorrectClientListenerException;
import org.infinispan.client.hotrod.exceptions.CacheNotTransactionalException;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheListenerException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.counter.exception.CounterException;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.Once;
import org.jboss.logging.annotations.Param;
import org.jboss.logging.annotations.ValidIdRange;

import io.netty.channel.Channel;

/**
 * Log abstraction for the hot rod client.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 4001, max = 5000)
public interface Log extends BasicLogger {
   String LOG_ROOT = "org.infinispan.";
   Log HOTROD = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, LOG_ROOT + "HOTROD");

   @LogMessage(level = WARN)
   @Message(value = "Could not find '%s' file in classpath, using defaults.", id = 4001)
   void couldNotFindPropertiesFile(String propertiesFile);

   @LogMessage(level = INFO)
   @Message(value = "Cannot perform operations on a cache associated with an unstarted RemoteCacheManager. Use RemoteCacheManager.start before using the remote cache.", id = 4002)
   void unstartedRemoteCacheManager();

   @Message(value = "Invalid magic number. Expected %#x and received %#x", id = 4003)
   InvalidResponseException invalidMagicNumber(short expectedMagicNumber, short receivedMagic);

//   @LogMessage(level = ERROR)
//   @Message(value = "Invalid message id. Expected %d and received %d", id = 4004)
//   void invalidMessageId(long expectedMsgId, long receivedMsgId);

   @LogMessage(level = WARN)
   @Message(value = "Error received from the server: %s", id = 4005)
   void errorFromServer(String message);

   @LogMessage(level = INFO)
   @Message(value = "Server sent new topology view (id=%d, age=%d) containing %d addresses: %s", id = 4006)
   void newTopology(int viewId, int age, int topologySize, Collection<? extends SocketAddress> addresses);

   @LogMessage(level = ERROR)
   @Message(value = "Exception encountered. Retry %d out of %d", id = 4007)
   void exceptionAndNoRetriesLeft(int retry, int maxRetries, @Cause Throwable te);

   //  id = 4008 is now logged to TRACE(ISPN-1794)

   // id = 4009 removed: errorClosingSocket

   @LogMessage(level = WARN)
   @Message(value = "No hash function configured for version: %d", id = 4011)
   void noHasHFunctionConfigured(int hashFunctionVersion);

   // id = 4012 removed: couldNoInvalidateConnection
   // id = 4013 removed: couldNotReleaseConnection

   @LogMessage(level = INFO)
   @Message(value = "New server added(%s), adding to the pool.", id = 4014)
   void newServerAdded(SocketAddress server);

//   @LogMessage(level = WARN)
//   @Message(value = "Failed adding new server %s", id = 4015)
//   void failedAddingNewServer(SocketAddress server, @Cause Throwable e);

   @LogMessage(level = INFO)
   @Message(value = "Server not in cluster anymore(%s), removing from the pool.", id = 4016)
   void removingServer(SocketAddress server);

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to convert string property [%s] to an int! Using default value of %d", id = 4018)
//   void unableToConvertStringPropertyToInt(String value, int defaultValue);
//
//   @LogMessage(level = WARN)
//   @Message(value = "Unable to convert string property [%s] to a long! Using default value of %d", id = 4019)
//   void unableToConvertStringPropertyToLong(String value, long defaultValue);
//
//   @LogMessage(level = WARN)
//   @Message(value = "Unable to convert string property [%s] to a boolean! Using default value of %b", id = 4020)
//   void unableToConvertStringPropertyToBoolean(String value, boolean defaultValue);

   @LogMessage(level = INFO)
   @Message(value = "Infinispan version: %s", id = 4021)
   void version(String version);

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

   @Message(value = "Cannot enable authentication without specifying either a username, a token, a client Subject or a CallbackHandler", id = 4030)
   CacheConfigurationException invalidAuthenticationConfiguration();

   @Message(value = "The selected authentication mechanism '%s' is not among the supported server mechanisms: %s", id = 4031)
   SecurityException unsupportedMech(String authMech, List<String> serverMechs);

//   @Message(value = "'%s' is an invalid SASL mechanism", id = 4032)
//   CacheConfigurationException invalidSaslMechanism(String saslMechanism);
//
//   @Message(value = "Connection dedicated to listener with id=%s but received event for listener with id=%s", id = 4033)
//   IllegalStateException unexpectedListenerId(String expectedListenerId, String receivedListenerId);

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
   void unexpectedErrorConsumingEvent(Object event, @Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "Unable to complete reading event from server %s", id = 4039)
   void unableToReadEventFromServer(@Cause Throwable t, SocketAddress server);

   @Message(value = "Cache listener class %s must be annotated with org.infinispan.client.hotrod.annotation.ClientListener", id = 4040)
   IncorrectClientListenerException missingClientListenerAnnotation(String className);

   @Message(value = "Unknown event type %s received", id = 4041)
   HotRodClientException unknownEvent(short eventTypeId);

//   @LogMessage(level = ERROR)
//   @Message(value = "Unable to set method %s accessible", id = 4042)
//   void unableToSetAccesible(Method m, @Cause Exception e);

//   @LogMessage(level = ERROR)
//   @Message(value = "Unrecoverable error reading event from server %s, exiting listener %s", id = 4043)
//   void unrecoverableErrorReadingEvent(@Cause Throwable t, SocketAddress server, String listenerId);
//
//   @LogMessage(level = ERROR)
//   @Message(value = "Unable to read %s bytes %s", id = 4044)
//   void unableToUnmarshallBytesError(String element, String bytes, @Cause Exception e);

   @Message(value = "When enabling near caching, number of max entries must be configured", id = 4045)
   CacheConfigurationException nearCacheMaxEntriesUndefined();

   @LogMessage(level = DEBUG)
   @Message(value = "Successfully closed remote iterator '%s'", id = 4046)
   void iterationClosed(String iterationId);

   @Message(value = "Invalid iteration id '%s'", id = 4047)
   IllegalStateException errorClosingIteration(String iterationId);

   @Message(value = "Invalid iteration id '%s'", id = 4048)
   NoSuchElementException errorRetrievingNext(String iterationId);

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

   @LogMessage(level = WARN)
   @Message(value = "Ignoring error when closing iteration '%s'", id = 4061)
   void ignoringErrorDuringIterationClose(String iterationId, @Cause Throwable e);

   @LogMessage(level = DEBUG)
   @Message(value = "Started iteration '%s'", id = 4062)
   void startedIteration(String iterationId);

   @LogMessage(level = DEBUG)
   @Message(value = "Channel to %s obtained for iteration '%s'", id = 4063)
   void iterationTransportObtained(SocketAddress address, String iterationId);

   @LogMessage(level = TRACE)
   @Message(value = "Tracking key %s belonging to segment %d, already tracked? = %b", id = 4064)
   void trackingSegmentKey(String key, int segment, boolean isTracked);

//   @LogMessage(level = WARN)
//   @Message(value = "Classpath does not look correct. Make sure you are not mixing uber and jars", id = 4065)
//   void warnAboutUberJarDuplicates();

//   @LogMessage(level = WARN)
//   @Message(value = "Unable to convert property [%s] to an enum! Using default value of %d", id = 4066)
//   void unableToConvertStringPropertyToEnum(String value, String defaultValue);

   @Message(value = "Cannot specify both a callback handler and a username/token for authentication", id = 4067)
   CacheConfigurationException callbackHandlerAndUsernameMutuallyExclusive();

//   @Message(value = "Class '%s' blocked by Java standard deserialization white list. Adjust the client configuration java serialization white list regular expression to include this class.", id = 4068)
//   CacheException classNotInWhitelist(String className);

   @Message(value = "Connection to %s is not active.", id = 4069)
   TransportException channelInactive(@Param SocketAddress address1, SocketAddress address2);

   @Message(value = "Failed to add client listener %s, server responded with status %d", id = 4070)
   HotRodClientException failedToAddListener(Object listener, short status);

   @Message(value = "Connection to %s was closed while waiting for response.", id = 4071)
   TransportException connectionClosed(@Param SocketAddress address1, SocketAddress address2);

   @LogMessage(level = ERROR)
   @Message(value = "Cannot create another async thread. Please increase 'infinispan.client.hotrod.default_executor_factory.pool_size' (current value is %d).", id = 4072)
   void cannotCreateAsyncThread(int maxPoolSize);

//   @LogMessage(level = WARN)
//   @Message(value = "TransportFactory is deprecated, this setting is not used anymore.", id = 4073)
//   void transportFactoryDeprecated();

   @LogMessage(level = INFO)
   @Message(value = "Native Epoll transport not available, using NIO instead: %s", id = 4074)
   void epollNotAvailable(String cause);

   @Message(value = "TrustStoreFileName and TrustStorePath are mutually exclusive", id = 4075)
   CacheConfigurationException trustStoreFileAndPathExclusive();

   @Message(value = "Unknown message id %d; cannot find matching request", id = 4076)
   IllegalStateException unknownMessageId(long messageId);

   @Message(value = "Closing channel %s due to error in unknown operation.", id = 4077)
   TransportException errorFromUnknownOperation(Channel channel, @Cause Throwable cause, @Param SocketAddress address);

   @Message(value = "This channel is about to be closed and does not accept any further operations.", id = 4078)
   HotRodClientException noMoreOperationsAllowed();

   @Message(value = "Unexpected listenerId %s", id = 4079)
   IllegalStateException unexpectedListenerId(String listenerId);

   @Message(value = "Event should use messageId of previous Add Client Listener operation but id is %d and operation is %s", id = 4080)
   IllegalStateException operationIsNotAddClientListener(long messageId, String operation);

   @Message(value = "TransactionMode must be non-null.", id = 4082)
   CacheConfigurationException invalidTransactionMode();

   @Message(value = "TransactionManagerLookup must be non-null", id = 4083)
   CacheConfigurationException invalidTransactionManagerLookup();

   @Message(value = "Cache %s doesn't support transactions. Please check the documentation how to configure it properly.", id = 4084)
   CacheNotTransactionalException cacheDoesNotSupportTransactions(String name);

   @LogMessage(level = ERROR)
   @Message(value = "Error checking server configuration for transactional cache %s", id = 4085)
   void invalidTxServerConfig(String name, @Cause Throwable throwable);

   @LogMessage(level = WARN)
   @Message(value = "Exception caught while preparing transaction %s", id = 4086)
   void exceptionDuringPrepare(Xid xid, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Use of maxIdle expiration with a near cache is unsupported.", id = 4087)
   void nearCacheMaxIdleUnsupported();

   @Message(value = "Transactions timeout must be positive", id = 4088)
   HotRodClientException invalidTransactionTimeout();

   @Message(value = "TransactionTable is not started!", id = 4089)
   HotRodClientException transactionTableNotStarted();

   @Message(value = "[%s] Invalid response operation. Expected %#x and received %#x", id = 4090)
   InvalidResponseException invalidResponse(String cacheName, short opRespCode, short receivedOpCode);

   @Message(value = "MBean registration failed", id = 4091)
   HotRodClientException jmxRegistrationFailure(@Cause Throwable cause);

   @Message(value = "MBean unregistration failed", id = 4092)
   HotRodClientException jmxUnregistrationFailure(@Cause Throwable cause);

   @Message(value = "OAUTHBEARER mechanism selected without providing a token", id = 4093)
   CacheConfigurationException oauthBearerWithoutToken();

   @Message(value = "Cannot specify both template name and configuration for '%s'", id = 4094)
   CacheConfigurationException remoteCacheTemplateNameXorConfiguration(String name);

   @Message(value = "Not a Hot Rod URI: %s", id = 4095)
   IllegalArgumentException notaHotRodURI(String uri);

   @Message(value = "Invalid property format in URI: %s", id = 4096)
   IllegalArgumentException invalidPropertyFormat(String part);

   @Message(value = "Illegal attempt to redefine an already existing cache configuration: %s", id = 4097)
   IllegalArgumentException duplicateCacheConfiguration(String name);

   @LogMessage(level = WARN)
   @Message(value = "Closing connection %s due to transport error", id = 4098)
   void closingChannelAfterError(Channel channel, @Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "Remote iteration over the entire result set of query '%s' without using pagination options is inefficient for large result sets. Please consider using 'startOffset' and 'maxResults' options.", id = 4099)
   void warnPerfRemoteIterationWithoutPagination(String query);

   @LogMessage(level = WARN)
   @Message(value = "Error reaching the server during iteration", id = 4100)
   void throwableDuringPublisher(@Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "Configuration property '%s' has been deprecated", id = 4101)
   void deprecatedConfigurationProperty(String property);

   @Message(value = "Near cache number of max entries must be a positive number when using bloom filter optimization, it was %d", id = 4102)
   CacheConfigurationException nearCacheMaxEntriesPositiveWithBloom(int maxEntries);

// @Message(value = "Near cache with bloom filter requires pool max active to be 1, was %s, and exhausted action to be WAIT, was %s", id = 4103)
// CacheConfigurationException bloomFilterRequiresMaxActiveOneAndWait(int maxActive, ExhaustedAction action);

   @LogMessage(level = WARN)
   @Message(value = "Failed to load and create an optional ProtoStream serialization context initializer: %s", id = 4104)
   void failedToCreatePredefinedSerializationContextInitializer(String className, @Cause Throwable throwable);

   @LogMessage(level = WARN)
   @Message(value = "Reverting to the initial server list for caches %s", id = 4105)
   void revertCacheToInitialServerList(Collection<String> cacheName);

   @LogMessage(level = WARN)
   @Message(value = "Invalid active count after closing channel %s", id = 4106)
   void invalidActiveCountAfterClose(Channel channel);

   @LogMessage(level = WARN)
   @Message(value = "Invalid created count after closing channel %s", id = 4107)
   void invalidCreatedCountAfterClose(Channel channel);

   @LogMessage(level = INFO)
   @Message(value = "Native IOUring transport not available, using NIO instead: %s", id = 4108)
   void ioUringNotAvailable(String cause);

   @LogMessage(level = TRACE)
   @Message(value = "OpenTelemetry API is not present in the classpath. Client context tracing will not be propagated.", id = 4109)
   void noOpenTelemetryAPI();

   @LogMessage(level = TRACE)
   @Message(value = "OpenTelemetry API is present in the classpath and the tracing propagation is enabled. Client context tracing will be propagated.", id = 4110)
   @Once
   void openTelemetryPropagationEnabled();

   @LogMessage(level = TRACE)
   @Message(value = "OpenTelemetry API is present in the classpath, but the tracing propagation is not enabled. Client context tracing will not be propagated.", id = 4111)
   @Once
   void openTelemetryPropagationDisabled();

   @Message(value = "The SNI hostname is required when hostname validation is enabled", id = 4112)
   CacheConfigurationException missingSniHostName();

   @LogMessage(level = INFO)
   @Message(value = "Query module not found. Queries are disabled.", id = 4113)
   void queryDisabled();

   @Message(value = "Query module not found. Add remote-query-client to the classpath.", id = 4114)
   HotRodClientException queryNotSupported();

   @LogMessage(level = WARN)
   @Message(value = "Unexpected error while creating the tracing propagation context. Client context tracing will not be propagated.", id = 4115)
   void errorCreatingPropagationContext(@Cause Throwable throwable);

   @LogMessage(level = WARN)
   @Message(value = "Client cannot marshall the server's key media type ('%s'). This could cause poor performance.", id = 4116)
   void serverKeyTypeNotRecognized(MediaType serverKeyType);

   @LogMessage(level = WARN)
   @Message(value = "Cache '%s' with marshaller %s does not handle server storage type '%s'. Configure the cache or default marshaller.", id = 4117)
   void invalidateNearDefaultMarshallerMismatch(String cacheName, Class<?> clazz, MediaType serverKeyType);

   @Message(value = CounterOutOfBoundsException.FORMAT_MESSAGE, id = 4118)
   CounterOutOfBoundsException counterOurOfBounds(String bound);

   @Message(value = "Invalid counter type. Expected=%s but got %s", id = 4119)
   CounterException invalidCounterType(String expected, String actual);

   @Message(value = "Counter '%s' is not defined.", id = 4120)
   CounterException undefinedCounter(String name);

}
