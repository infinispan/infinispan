package org.infinispan.client.hotrod.impl;

import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.TransportFactory;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ExhaustedAction;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.configuration.StatisticsConfiguration;
import org.infinispan.client.hotrod.configuration.TransactionConfigurationBuilder;
import org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.commons.util.TypedProperties;

/**
 * Encapsulate all config properties here
 *
 * @author Manik Surtani
 * @version 4.1
 */
public class ConfigurationProperties {
   static final String ICH = "infinispan.client.hotrod.";
   public static final String URI = ICH + "uri";
   public static final String SERVER_LIST = ICH + "server_list";
   public static final String MARSHALLER = ICH + "marshaller";
   public static final String CONTEXT_INITIALIZERS = ICH + "context-initializers";
   public static final String ASYNC_EXECUTOR_FACTORY = ICH + "async_executor_factory";
   public static final String CLIENT_INTELLIGENCE = ICH + "client_intelligence";
   public static final String DEFAULT_EXECUTOR_FACTORY_POOL_SIZE = ICH + "default_executor_factory.pool_size";
   public static final String DEFAULT_EXECUTOR_FACTORY_THREADNAME_PREFIX = ICH + "default_executor_factory.threadname_prefix";
   public static final String DEFAULT_EXECUTOR_FACTORY_THREADNAME_SUFFIX = ICH + "default_executor_factory.threadname_suffix";
   public static final String TCP_NO_DELAY = ICH + "tcp_no_delay";
   public static final String TCP_KEEP_ALIVE = ICH + "tcp_keep_alive";
   public static final String REQUEST_BALANCING_STRATEGY = ICH + "request_balancing_strategy";
   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public static final String KEY_SIZE_ESTIMATE = ICH + "key_size_estimate";
   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public static final String VALUE_SIZE_ESTIMATE = ICH + "value_size_estimate";
   public static final String FORCE_RETURN_VALUES = ICH + "force_return_values";
   public static final String HASH_FUNCTION_PREFIX = ICH + "hash_function_impl";
   // Connection properties
   public static final String SO_TIMEOUT = ICH + "socket_timeout";
   public static final String CONNECT_TIMEOUT = ICH + "connect_timeout";
   public static final String PROTOCOL_VERSION = ICH + "protocol_version";
   public static final String TRANSPORT_FACTORY = ICH + "transport_factory";
   public static final String SERVER_FAILED_TIMEOUT = ICH + "server_failed_timeout";
   // Encryption properties
   public static final String USE_SSL = ICH + "use_ssl";
   public static final String KEY_STORE_FILE_NAME = ICH + "key_store_file_name";
   public static final String KEY_STORE_TYPE = ICH + "key_store_type";
   public static final String KEY_STORE_PASSWORD = ICH + "key_store_password";
   public static final String SNI_HOST_NAME = ICH + "sni_host_name";
   public static final String KEY_ALIAS = ICH + "key_alias";
   public static final String TRUST_STORE_FILE_NAME = ICH + "trust_store_file_name";
   public static final String TRUST_STORE_PATH = ICH + "trust_store_path";
   public static final String TRUST_STORE_TYPE = ICH + "trust_store_type";
   public static final String TRUST_STORE_PASSWORD = ICH + "trust_store_password";
   public static final String SSL_PROVIDER = ICH + "ssl_provider";
   public static final String SSL_PROTOCOL = ICH + "ssl_protocol";
   public static final String SSL_CIPHERS = ICH + "ssl_ciphers";
   public static final String SSL_CONTEXT = ICH + "ssl_context";
   public static final String SSL_HOSTNAME_VALIDATION = ICH + "ssl_hostname_validation";
   public static final String MAX_RETRIES = ICH + "max_retries";
   // Authentication properties
   public static final String USE_AUTH = ICH + "use_auth";
   public static final String SASL_MECHANISM = ICH + "sasl_mechanism";
   public static final String AUTH_CALLBACK_HANDLER = ICH + "auth_callback_handler";
   public static final String AUTH_SERVER_NAME = ICH + "auth_server_name";
   public static final String AUTH_USERNAME = ICH + "auth_username";
   public static final String AUTH_PASSWORD = ICH + "auth_password";
   public static final String AUTH_TOKEN = ICH + "auth_token";
   public static final String AUTH_REALM = ICH + "auth_realm";
   public static final String AUTH_CLIENT_SUBJECT = ICH + "auth_client_subject";
   public static final String SASL_PROPERTIES_PREFIX = ICH + "sasl_properties";
   public static final Pattern SASL_PROPERTIES_PREFIX_REGEX =
         Pattern.compile('^' + ConfigurationProperties.SASL_PROPERTIES_PREFIX + '.');
   public static final String JAVA_SERIAL_ALLOWLIST = ICH + "java_serial_allowlist";
   @Deprecated(forRemoval=true, since = "12.0")
   public static final String JAVA_SERIAL_WHITELIST = ICH + "java_serial_whitelist";
   public static final String BATCH_SIZE = ICH + "batch_size";
   // Statistics properties
   public static final String STATISTICS = ICH + "statistics";
   public static final String JMX = ICH + "jmx";
   public static final String JMX_NAME = ICH + "jmx_name";
   public static final String JMX_DOMAIN = ICH + "jmx_domain";
   // Transaction properties
   public static final String TRANSACTION_MANAGER_LOOKUP = ICH + "transaction.transaction_manager_lookup";
   public static final String TRANSACTION_MODE = ICH + "transaction.transaction_mode";
   public static final String TRANSACTION_TIMEOUT = ICH + "transaction.timeout";
   // Near cache properties
   public static final String NEAR_CACHE_MAX_ENTRIES = ICH + "near_cache.max_entries";
   public static final String NEAR_CACHE_MODE = ICH + "near_cache.mode";
   public static final String NEAR_CACHE_BLOOM_FILTER = ICH + "near_cache.bloom_filter";
   public static final String NEAR_CACHE_NAME_PATTERN = ICH + "near_cache.name_pattern";
   // Pool properties
   public static final String CONNECTION_POOL_MAX_ACTIVE = ICH + "connection_pool.max_active";
   public static final String CONNECTION_POOL_MAX_WAIT = ICH + "connection_pool.max_wait";
   public static final String CONNECTION_POOL_MIN_IDLE = ICH + "connection_pool.min_idle";
   public static final String CONNECTION_POOL_MAX_PENDING_REQUESTS = ICH + "connection_pool.max_pending_requests";
   public static final String CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME = ICH + "connection_pool.min_evictable_idle_time";
   public static final String CONNECTION_POOL_EXHAUSTED_ACTION = ICH + "connection_pool.exhausted_action";
   // XSite properties
   public static final String CLUSTER_PROPERTIES_PREFIX = ICH + "cluster";
   public static final Pattern CLUSTER_PROPERTIES_PREFIX_REGEX =
         Pattern.compile('^' + ConfigurationProperties.CLUSTER_PROPERTIES_PREFIX + '.');
   public static final Pattern CLUSTER_PROPERTIES_PREFIX_INTELLIGENCE_REGEX =
         Pattern.compile('^' + ConfigurationProperties.CLUSTER_PROPERTIES_PREFIX + '.' + "intelligence.");
   // Tracing properties
   public static final String TRACING_PROPAGATION_ENABLED = ICH + "tracing.propagation_enabled";
   // Cache properties
   public static final String CACHE_PREFIX= ICH + "cache.";
   public static final String CACHE_CONFIGURATION_SUFFIX = ".configuration";
   public static final String CACHE_CONFIGURATION_URI_SUFFIX = ".configuration_uri";
   public static final String CACHE_FORCE_RETURN_VALUES_SUFFIX = ".force_return_values";
   public static final String CACHE_MARSHALLER = ".marshaller";
   public static final String CACHE_NEAR_CACHE_MODE_SUFFIX = ".near_cache.mode";
   public static final String CACHE_NEAR_CACHE_MAX_ENTRIES_SUFFIX = ".near_cache.max_entries";
   public static final String CACHE_NEAR_CACHE_FACTORY_SUFFIX = ".near_cache.factory";
   public static final String CACHE_NEAR_CACHE_BLOOM_FILTER_SUFFIX = ".near_cache.bloom_filter";
   public static final String CACHE_TEMPLATE_NAME_SUFFIX = ".template_name";
   public static final String CACHE_TRANSACTION_MODE_SUFFIX = ".transaction.transaction_mode";
   public static final String CACHE_TRANSACTION_MANAGER_LOOKUP_SUFFIX = ".transaction.transaction_manager_lookup";
   public static final String DNS_RESOLVER_MIN_TTL = ".dns_resolver_min_ttl";
   public static final String DNS_RESOLVER_MAX_TTL = ".dns_resolver_max_ttl";
   public static final String DNS_RESOLVER_NEGATIVE_TTL = ".dns_resolver_negative_ttl";

   // defaults
   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public static final int DEFAULT_KEY_SIZE = 64;
   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public static final int DEFAULT_VALUE_SIZE = 512;
   public static final int DEFAULT_HOTROD_PORT = 11222;
   public static final int DEFAULT_SO_TIMEOUT = 2_000;
   public static final int DEFAULT_CONNECT_TIMEOUT = 2_000;
   public static final int DEFAULT_MAX_RETRIES = 3;
   public static final int DEFAULT_BATCH_SIZE = 10_000;
   public static final int DEFAULT_MAX_PENDING_REQUESTS = 5;
   public static final long DEFAULT_MIN_EVICTABLE_IDLE_TIME = 180000L;
   public static final int DEFAULT_MAX_ACTIVE = -1;
   public static final int DEFAULT_MAX_WAIT = -1;
   public static final int DEFAULT_MIN_IDLE = -1;
   public static final boolean DEFAULT_TRACING_PROPAGATION_ENABLED = true;
   public static final int DEFAULT_SERVER_FAILED_TIMEOUT = 30_000;

   private final TypedProperties props;


   public ConfigurationProperties() {
      this.props = new TypedProperties();
   }

   public ConfigurationProperties(String serverList) {
      this();
      setServerList(serverList);
   }

   public ConfigurationProperties(Properties props) {
      this.props = props == null ? new TypedProperties() : TypedProperties.toTypedProperties(props);
   }

   public void setURI(String uri) {
      props.setProperty(URI, uri);
   }

   public String getURI() {
      return props.getProperty(URI);
   }

   public void setServerList(String serverList) {
      props.setProperty(SERVER_LIST, serverList);
   }

   public String getMarshaller() {
      return props.getProperty(MARSHALLER);
   }

   public void setMarshaller(String marshaller) {
      props.setProperty(MARSHALLER, marshaller);
   }

   public String getContextInitializers() {
      return props.getProperty(CONTEXT_INITIALIZERS);
   }

   public void setContextInitializers(String contextInitializers) {
      props.setProperty(CONTEXT_INITIALIZERS, contextInitializers);
   }

   public String getAsyncExecutorFactory() {
      return props.getProperty(ASYNC_EXECUTOR_FACTORY, DefaultAsyncExecutorFactory.class.getName());
   }

   public int getDefaultExecutorFactoryPoolSize() {
      return props.getIntProperty(DEFAULT_EXECUTOR_FACTORY_POOL_SIZE, 99);
   }

   public void setDefaultExecutorFactoryPoolSize(int poolSize) {
      props.setProperty(DEFAULT_EXECUTOR_FACTORY_POOL_SIZE, poolSize);
   }

   public String getDefaultExecutorFactoryThreadNamePrefix() {
      return props.getProperty(DEFAULT_EXECUTOR_FACTORY_THREADNAME_PREFIX, DefaultAsyncExecutorFactory.THREAD_NAME);
   }

   public void setDefaultExecutorFactoryThreadNamePrefix(String threadNamePrefix) {
      props.setProperty(DEFAULT_EXECUTOR_FACTORY_THREADNAME_PREFIX, threadNamePrefix);
   }

   public String getDefaultExecutorFactoryThreadNameSuffix() {
      return props.getProperty(DEFAULT_EXECUTOR_FACTORY_THREADNAME_SUFFIX, "");
   }

   public void setDefaultExecutorFactoryThreadNameSuffix(String threadNameSuffix) {
      props.setProperty(DEFAULT_EXECUTOR_FACTORY_THREADNAME_SUFFIX, threadNameSuffix);
   }

   public void setTransportFactory(String transportFactoryClass) {
      props.setProperty(TRANSPORT_FACTORY, transportFactoryClass);
   }

   public void setTransportFactory(Class<TransportFactory> transportFactory) {
      setTransportFactory(transportFactory.getName());
   }

   public String getTransportFactory() {
      return props.getProperty(TRANSPORT_FACTORY, TransportFactory.DEFAULT.getClass().getName());
   }

   public boolean getTcpNoDelay() {
      return props.getBooleanProperty(TCP_NO_DELAY, true);
   }

   public void setTcpNoDelay(boolean tcpNoDelay) {
      props.setProperty(TCP_NO_DELAY, tcpNoDelay);
   }

   public boolean getTcpKeepAlive() {
      return props.getBooleanProperty(TCP_KEEP_ALIVE, false);
   }

   public void setTcpKeepAlive(boolean tcpKeepAlive) {
      props.setProperty(TCP_KEEP_ALIVE, tcpKeepAlive);
   }

   public String getRequestBalancingStrategy() {
      return props.getProperty(REQUEST_BALANCING_STRATEGY, RoundRobinBalancingStrategy.class.getName());
   }

   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public int getKeySizeEstimate() {
      return props.getIntProperty(KEY_SIZE_ESTIMATE, DEFAULT_KEY_SIZE);
   }

   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public void setKeySizeEstimate(int keySizeEstimate) {
      props.setProperty(KEY_SIZE_ESTIMATE, keySizeEstimate);
   }

   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public int getValueSizeEstimate() {
      return props.getIntProperty(VALUE_SIZE_ESTIMATE, DEFAULT_VALUE_SIZE);
   }

   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public void setValueSizeEstimate(int valueSizeEstimate) {
      props.setProperty(VALUE_SIZE_ESTIMATE, valueSizeEstimate);
   }

   public boolean getForceReturnValues() {
      return props.getBooleanProperty(FORCE_RETURN_VALUES, false);
   }

   public void setForceReturnValues(boolean forceReturnValues) {
      props.setProperty(FORCE_RETURN_VALUES, forceReturnValues);
   }

   public Properties getProperties() {
      return props;
   }

   public int getSoTimeout() {
      return props.getIntProperty(SO_TIMEOUT, DEFAULT_SO_TIMEOUT);
   }

   public void setSocketTimeout(int socketTimeout) {
      props.setProperty(SO_TIMEOUT, socketTimeout);
   }

   public String getProtocolVersion() {
      return props.getProperty(PROTOCOL_VERSION, ProtocolVersion.DEFAULT_PROTOCOL_VERSION.toString());
   }

   public void setProtocolVersion(String protocolVersion) {
      props.setProperty(PROTOCOL_VERSION, protocolVersion);
   }

   public String getClientIntelligence() {
      return props.getProperty(CLIENT_INTELLIGENCE, ClientIntelligence.getDefault().name());
   }

   public void setClientIntelligence(String clientIntelligence) {
      props.setProperty(CLIENT_INTELLIGENCE, clientIntelligence);
   }

   public int getConnectTimeout() {
      return props.getIntProperty(CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
   }

   public void setConnectTimeout(int connectTimeout) {
      props.setProperty(CONNECT_TIMEOUT, connectTimeout);
   }

   public boolean getUseSSL() {
      return props.getBooleanProperty(USE_SSL, false);
   }

   public void setUseSSL(boolean useSSL) {
      props.setProperty(USE_SSL, useSSL);
   }

   public String getKeyStoreFileName() {
      return props.getProperty(KEY_STORE_FILE_NAME);
   }

   public void setKeyStoreFileName(String keyStoreFileName) {
      props.setProperty(KEY_STORE_FILE_NAME, keyStoreFileName);
   }

   public String getKeyStoreType() {
      return props.getProperty(KEY_STORE_TYPE);
   }

   public void setKeyStoreType(String keyStoreType) {
      props.setProperty(KEY_STORE_TYPE, keyStoreType);
   }

   public String getKeyStorePassword() {
      return props.getProperty(KEY_STORE_PASSWORD);
   }

   public void setKeyStorePassword(String keyStorePassword) {
      props.setProperty(KEY_STORE_PASSWORD, keyStorePassword);
   }

   public String getKeyAlias() {
      return props.getProperty(KEY_ALIAS);
   }

   public void setKeyAlias(String keyAlias) {
      props.setProperty(KEY_ALIAS, keyAlias);
   }

   public String getTrustStoreFileName() {
      return props.getProperty(TRUST_STORE_FILE_NAME);
   }

   public void setTrustStoreFileName(String trustStoreFileName) {
      props.setProperty(TRUST_STORE_FILE_NAME, trustStoreFileName);
   }

   public String getTrustStoreType() {
      return props.getProperty(TRUST_STORE_TYPE);
   }

   public void setTrustStoreType(String trustStoreType) {
      props.setProperty(TRUST_STORE_TYPE, trustStoreType);
   }

   public String getTrustStorePassword() {
      return props.getProperty(TRUST_STORE_PASSWORD);
   }

   public void setTrustStorePassword(String trustStorePassword) {
      props.setProperty(TRUST_STORE_PASSWORD, trustStorePassword);
   }

   /**
    * @deprecated Since 12.0 and will be removed in 15.0
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public String getTrustStorePath() {
      return props.getProperty(TRUST_STORE_PATH);
   }

   /**
    * @deprecated Since 12.0 and will be removed in 15.0
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public void setTrustStorePath(String trustStorePath) {
      props.setProperty(TRUST_STORE_PATH, trustStorePath);
   }

   public String getSSLProtocol() {
      return props.getProperty(SSL_PROTOCOL);
   }

   public void setSSLProtocol(String sslProtocol) {
      props.setProperty(SSL_PROTOCOL, sslProtocol);
   }

   public String getSniHostName() {
      return props.getProperty(SNI_HOST_NAME);
   }

   public void setSniHostName(String sniHostName) {
      props.setProperty(SNI_HOST_NAME, sniHostName);
   }

   public boolean getUseAuth() {
      return props.getBooleanProperty(USE_AUTH, false);
   }

   public void setUseAuth(boolean useAuth) {
      props.setProperty(USE_AUTH, useAuth);
   }

   public String getSaslMechanism() {
      return props.getProperty(SASL_MECHANISM);
   }

   public void setSaslMechanism(String saslMechanism) {
      props.setProperty(SASL_MECHANISM, saslMechanism);
   }

   public String getAuthUsername() {
      return props.getProperty(AUTH_USERNAME);
   }

   public void setAuthUsername(String authUsername) {
      props.setProperty(AUTH_USERNAME, authUsername);
   }

   public String getAuthPassword() {
      return props.getProperty(AUTH_PASSWORD);
   }

   public void setAuthPassword(String authPassword) {
      props.setProperty(AUTH_PASSWORD, authPassword);
   }

   public String getAuthToken() {
      return props.getProperty(AUTH_TOKEN);
   }

   public void setAuthToken(String authToken) {
      props.setProperty(AUTH_TOKEN, authToken);
   }

   public String getAuthRealm() {
      return props.getProperty(AUTH_REALM);
   }

   public void setAuthRealm(String authRealm) {
      props.setProperty(AUTH_REALM, authRealm);
   }

   public void setAuthServerName(String authServerName) {
      props.setProperty(AUTH_SERVER_NAME, authServerName);
   }

   public int getMaxRetries() {
      return props.getIntProperty(MAX_RETRIES, DEFAULT_MAX_RETRIES);
   }

   public void setMaxRetries(int maxRetries) {
      props.setProperty(MAX_RETRIES, maxRetries);
   }

   public int getBatchSize() {
      return props.getIntProperty(BATCH_SIZE, DEFAULT_BATCH_SIZE);
   }

   public void setBatchSize(int batchSize) {
      props.setProperty(BATCH_SIZE, batchSize);
   }

   public void setStatistics(boolean statistics) {
      props.setProperty(STATISTICS, statistics);
   }

   public boolean isStatistics() {
      return props.getBooleanProperty(STATISTICS, StatisticsConfiguration.ENABLED.getDefaultValue());
   }

   public void setJmx(boolean jmx) {
      props.setProperty(JMX, jmx);
   }

   public boolean isJmx() {
      return props.getBooleanProperty(JMX, StatisticsConfiguration.JMX_ENABLED.getDefaultValue());
   }

   public void setJmxName(String jmxName) {
      props.setProperty(JMX_NAME, jmxName);
   }

   public void getJmxName() {
      props.getProperty(JMX_NAME);
   }

   public void setJmxDomain(String jmxDomain) {
      props.setProperty(JMX_DOMAIN, jmxDomain);
   }

   public void getJmxDomain() {
      props.getProperty(JMX_DOMAIN);
   }

   public String getTransactionManagerLookup() {
      return props.getProperty(TRANSACTION_MANAGER_LOOKUP, TransactionConfigurationBuilder.defaultTransactionManagerLookup().getClass().getName(), true);
   }

   public NearCacheMode getNearCacheMode() {
      return props.getEnumProperty(NEAR_CACHE_MODE, NearCacheMode.class, NearCacheMode.DISABLED, true);
   }

   public void setNearCacheMode(String nearCacheMode) {
      props.setProperty(NEAR_CACHE_MODE, nearCacheMode);
   }

   public int getNearCacheMaxEntries() {
      return props.getIntProperty(NEAR_CACHE_MAX_ENTRIES, -1);
   }

   public void setNearCacheMaxEntries(int nearCacheMaxEntries) {
      props.setProperty(NEAR_CACHE_MAX_ENTRIES, nearCacheMaxEntries);
   }

   @Deprecated(forRemoval=true, since = "11.0")
   public String getNearCacheNamePattern() {
      return props.getProperty(NEAR_CACHE_NAME_PATTERN);
   }

   @Deprecated(forRemoval=true, since = "11.0")
   public void setNearCacheNamePattern(String nearCacheNamePattern) {
      props.setProperty(NEAR_CACHE_NAME_PATTERN, nearCacheNamePattern);
   }

   public int getConnectionPoolMaxActive() {
      return props.getIntProperty(CONNECTION_POOL_MAX_ACTIVE, DEFAULT_MAX_ACTIVE);
   }

   public void setConnectionPoolMaxActive(int connectionPoolMaxActive) {
      props.setProperty(CONNECTION_POOL_MAX_ACTIVE, connectionPoolMaxActive);
   }

   public long getConnectionPoolMaxWait() {
      return props.getLongProperty(CONNECTION_POOL_MAX_WAIT, DEFAULT_MAX_WAIT);
   }

   public void setConnectionPoolMaxWait(long connectionPoolMaxWait) {
      props.setProperty(CONNECTION_POOL_MAX_WAIT, connectionPoolMaxWait);
   }

   public int gtConnectionPoolMinIdle() {
      return props.getIntProperty(CONNECTION_POOL_MIN_IDLE, DEFAULT_MIN_IDLE);
   }

   public void setConnectionPoolMinIdle(int connectionPoolMinIdle) {
      props.setProperty(CONNECTION_POOL_MIN_IDLE, connectionPoolMinIdle);
   }

   public int getConnectionPoolMaxPendingRequests() {
      return props.getIntProperty(CONNECTION_POOL_MAX_PENDING_REQUESTS, DEFAULT_MAX_PENDING_REQUESTS);
   }

   public void setConnectionPoolMaxPendingRequests(int connectionPoolMaxPendingRequests) {
      props.setProperty(CONNECTION_POOL_MAX_PENDING_REQUESTS, connectionPoolMaxPendingRequests);
   }

   public long setConnectionPoolMinEvictableIdleTime() {
      return props.getLongProperty(CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME, DEFAULT_MIN_EVICTABLE_IDLE_TIME);
   }

   public void setConnectionPoolMinEvictableIdleTime(long connectionPoolMinEvictableIdleTime) {
      props.setProperty(CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME, connectionPoolMinEvictableIdleTime);
   }

   public ExhaustedAction getConnectionPoolExhaustedAction() {
      return props.getEnumProperty(CONNECTION_POOL_EXHAUSTED_ACTION, ExhaustedAction.class, ExhaustedAction.WAIT);
   }

   public void setConnectionPoolExhaustedAction(String connectionPoolExhaustedAction) {
      props.setProperty(CONNECTION_POOL_EXHAUSTED_ACTION, connectionPoolExhaustedAction);
   }

   public boolean isTracingPropagationEnabled() {
      return props.getBooleanProperty(TRACING_PROPAGATION_ENABLED, DEFAULT_TRACING_PROPAGATION_ENABLED);
   }

   public void setTracingPropagationEnabled(boolean tracingPropagationEnabled) {
      props.setProperty(TRACING_PROPAGATION_ENABLED, tracingPropagationEnabled);
   }

   /**
    * Is version previous to, and not including, 1.2?
    */
   public static boolean isVersionPre12(Configuration cfg) {
      String version = cfg.version().toString();
      return Objects.equals(version, "1.0") || Objects.equals(version, "1.1");
   }

   public String getServerList(){
      return props.getProperty(SERVER_LIST);
   }

   /**
    * @deprecated Use {@link #setJavaSerialAllowList(String)} instead. To be removed in 14.0.
    * @param javaSerialWhitelist
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public void setJavaSerialWhitelist(String javaSerialWhitelist) {
      setJavaSerialAllowList(javaSerialWhitelist);
   }

   public void setJavaSerialAllowList(String javaSerialAllowlist) {
      props.setProperty(JAVA_SERIAL_ALLOWLIST, javaSerialAllowlist);
   }

   public void setTransactionMode(String transactionMode) {
      props.setProperty(TRANSACTION_MODE, transactionMode);
   }

   public String getTransactionMode() {
      return props.getProperty(TRANSACTION_MODE);
   }

   public void setTransactionTimeout(int transactionTimeout) {
      props.setProperty(TRANSACTION_TIMEOUT, transactionTimeout);
   }

   public int getTransactionTimeout() {
      return props.getIntProperty(TRANSACTION_TIMEOUT, DEFAULT_SO_TIMEOUT);
   }
}
