package org.infinispan.client.hotrod.impl;

import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.configuration.TransactionConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;

/**
 * Encapsulate all config properties here
 *
 * @author Manik Surtani
 * @version 4.1
 */
public class ConfigurationProperties {
   @Deprecated
   public static final String TRANSPORT_FACTORY = "infinispan.client.hotrod.transport_factory";
   public static final String SERVER_LIST = "infinispan.client.hotrod.server_list";
   public static final String MARSHALLER = "infinispan.client.hotrod.marshaller";
   public static final String ASYNC_EXECUTOR_FACTORY = "infinispan.client.hotrod.async_executor_factory";
   public static final String CLIENT_INTELLIGENCE = "infinispan.client.hotrod.client_intelligence";
   public static final String DEFAULT_EXECUTOR_FACTORY_POOL_SIZE = "infinispan.client.hotrod.default_executor_factory.pool_size";
   public static final String TCP_NO_DELAY = "infinispan.client.hotrod.tcp_no_delay";
   public static final String TCP_KEEP_ALIVE = "infinispan.client.hotrod.tcp_keep_alive";
   @Deprecated
   public static final String PING_ON_STARTUP = "infinispan.client.hotrod.ping_on_startup";
   public static final String REQUEST_BALANCING_STRATEGY = "infinispan.client.hotrod.request_balancing_strategy";
   public static final String KEY_SIZE_ESTIMATE = "infinispan.client.hotrod.key_size_estimate";
   public static final String VALUE_SIZE_ESTIMATE = "infinispan.client.hotrod.value_size_estimate";
   public static final String FORCE_RETURN_VALUES = "infinispan.client.hotrod.force_return_values";
   public static final String HASH_FUNCTION_PREFIX = "infinispan.client.hotrod.hash_function_impl";
   @Deprecated
   public static final String DEFAULT_EXECUTOR_FACTORY_QUEUE_SIZE = "infinispan.client.hotrod.default_executor_factory.queue_size";
   public static final String SO_TIMEOUT = "infinispan.client.hotrod.socket_timeout";
   public static final String CONNECT_TIMEOUT = "infinispan.client.hotrod.connect_timeout";
   public static final String PROTOCOL_VERSION = "infinispan.client.hotrod.protocol_version";
   public static final String USE_SSL = "infinispan.client.hotrod.use_ssl";
   public static final String KEY_STORE_FILE_NAME = "infinispan.client.hotrod.key_store_file_name";
   public static final String KEY_STORE_TYPE = "infinispan.client.hotrod.key_store_type";
   public static final String KEY_STORE_PASSWORD = "infinispan.client.hotrod.key_store_password";
   public static final String SNI_HOST_NAME = "infinispan.client.hotrod.sni_host_name";
   public static final String KEY_ALIAS = "infinispan.client.hotrod.key_alias";
   public static final String KEY_STORE_CERTIFICATE_PASSWORD = "infinispan.client.hotrod.key_store_certificate_password";
   public static final String TRUST_STORE_FILE_NAME = "infinispan.client.hotrod.trust_store_file_name";
   public static final String TRUST_STORE_PATH = "infinispan.client.hotrod.trust_store_path";
   public static final String TRUST_STORE_TYPE = "infinispan.client.hotrod.trust_store_type";
   public static final String TRUST_STORE_PASSWORD = "infinispan.client.hotrod.trust_store_password";
   public static final String SSL_PROTOCOL = "infinispan.client.hotrod.ssl_protocol";
   public static final String SSL_CONTEXT = "infinispan.client.hotrod.ssl_context";
   public static final String MAX_RETRIES = "infinispan.client.hotrod.max_retries";
   public static final String USE_AUTH = "infinispan.client.hotrod.use_auth";
   public static final String SASL_MECHANISM = "infinispan.client.hotrod.sasl_mechanism";
   public static final String AUTH_CALLBACK_HANDLER = "infinispan.client.hotrod.auth_callback_handler";
   public static final String AUTH_SERVER_NAME = "infinispan.client.hotrod.auth_server_name";
   public static final String AUTH_USERNAME = "infinispan.client.hotrod.auth_username";
   public static final String AUTH_PASSWORD = "infinispan.client.hotrod.auth_password";
   public static final String AUTH_REALM = "infinispan.client.hotrod.auth_realm";
   public static final String AUTH_CLIENT_SUBJECT = "infinispan.client.hotrod.auth_client_subject";
   public static final String SASL_PROPERTIES_PREFIX = "infinispan.client.hotrod.sasl_properties";
   public static final Pattern SASL_PROPERTIES_PREFIX_REGEX =
         Pattern.compile('^' + ConfigurationProperties.SASL_PROPERTIES_PREFIX + '.');
   public static final String JAVA_SERIAL_WHITELIST = "infinispan.client.hotrod.java_serial_whitelist";
   public static final String BATCH_SIZE = "infinispan.client.hotrod.batch_size";
   public static final String TRANSACTION_MANAGER_LOOKUP = "infinispan.client.hotrod.transaction.transaction_manager_lookup";
   public static final String TRANSACTION_MODE = "infinispan.client.hotrod.transaction.transaction_mode";
   public static final String TRANSACTION_TIMEOUT = "infinispan.client.hotrod.transaction.timeout";
   public static final String NEAR_CACHE_MAX_ENTRIES = "infinispan.client.hotrod.near_cache.max_entries";
   public static final String NEAR_CACHE_MODE = "infinispan.client.hotrod.near_cache.mode";
   public static final String NEAR_CACHE_NAME_PATTERN = "infinispan.client.hotrod.near_cache.name_pattern";

   // defaults

   public static final int DEFAULT_KEY_SIZE = 64;
   public static final int DEFAULT_VALUE_SIZE = 512;
   public static final int DEFAULT_HOTROD_PORT = 11222;
   public static final int DEFAULT_SO_TIMEOUT = 60000;
   public static final int DEFAULT_CONNECT_TIMEOUT = 60000;
   public static final int DEFAULT_MAX_RETRIES = 10;
   public static final int DEFAULT_BATCH_SIZE = 10000;

   private final TypedProperties props;


   public ConfigurationProperties() {
      this.props = new TypedProperties();
   }

   public ConfigurationProperties(String serverList) {
      this();
      props.setProperty(SERVER_LIST, serverList);
   }

   public ConfigurationProperties(Properties props) {
      this.props = props == null ? new TypedProperties() : TypedProperties.toTypedProperties(props);
   }

   public String getTransportFactory() {
      return props.getProperty(TRANSPORT_FACTORY, ChannelFactory.class.getName());
   }

   public String getMarshaller() {
      return props.getProperty(MARSHALLER, GenericJBossMarshaller.class.getName());
   }

   public String getAsyncExecutorFactory() {
      return props.getProperty(ASYNC_EXECUTOR_FACTORY, DefaultAsyncExecutorFactory.class.getName());
   }

   public int getDefaultExecutorFactoryPoolSize() {
      return props.getIntProperty(DEFAULT_EXECUTOR_FACTORY_POOL_SIZE, 99);
   }

   public boolean getTcpNoDelay() {
      return props.getBooleanProperty(TCP_NO_DELAY, true);
   }

   public boolean getTcpKeepAlive() {
      return props.getBooleanProperty(TCP_KEEP_ALIVE, false);
   }

   public String getRequestBalancingStrategy() {
      return props.getProperty(REQUEST_BALANCING_STRATEGY, RoundRobinBalancingStrategy.class.getName());
   }

   public int getKeySizeEstimate() {
      return props.getIntProperty(KEY_SIZE_ESTIMATE, DEFAULT_KEY_SIZE);
   }

   public int getValueSizeEstimate() {
      return props.getIntProperty(VALUE_SIZE_ESTIMATE, DEFAULT_VALUE_SIZE);
   }

   public boolean getForceReturnValues() {
      return props.getBooleanProperty(FORCE_RETURN_VALUES, false);
   }

   public Properties getProperties() {
      return props;
   }

   public int getSoTimeout() {
      return props.getIntProperty(SO_TIMEOUT, DEFAULT_SO_TIMEOUT);
   }

   public String getProtocolVersion() {
      return props.getProperty(PROTOCOL_VERSION, ProtocolVersion.DEFAULT_PROTOCOL_VERSION.toString());
   }

   public int getConnectTimeout() {
      return props.getIntProperty(CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
   }

   public boolean getUseSSL() {
      return props.getBooleanProperty(USE_SSL, false);
   }

   public String getKeyStoreFileName() {
      return props.getProperty(KEY_STORE_FILE_NAME, null);
   }

   public String getKeyStoreType() {
      return props.getProperty(KEY_STORE_TYPE, null);
   }

   public String getKeyStorePassword() {
      return props.getProperty(KEY_STORE_PASSWORD, null);
   }

   public String getKeyAlias() {
      return props.getProperty(KEY_ALIAS, null);
   }

   public String getTrustStoreFileName() {
      return props.getProperty(TRUST_STORE_FILE_NAME, null);
   }

   public String getTrustStoreType() {
      return props.getProperty(TRUST_STORE_TYPE, null);
   }

   public String getTrustStorePassword() {
      return props.getProperty(TRUST_STORE_PASSWORD, null);
   }

   public String getSSLProtocol() {
      return props.getProperty(SSL_PROTOCOL, null);
   }

   public int getMaxRetries() {
      return props.getIntProperty(MAX_RETRIES, DEFAULT_MAX_RETRIES);
   }

   public int getBatchSize() {
      return props.getIntProperty(BATCH_SIZE, DEFAULT_BATCH_SIZE);
   }

   public String getTransactionManagerLookup() {
      return props.getProperty(TRANSACTION_MANAGER_LOOKUP, TransactionConfigurationBuilder.defaultTransactionManagerLookup().getClass().getName(), true);
   }

   public TransactionMode getTransactionMode() {
      return props.getEnumProperty(TRANSACTION_MODE, TransactionMode.class, TransactionMode.NONE, true);
   }

   public NearCacheMode getNearCacheMode() {
      return props.getEnumProperty(NEAR_CACHE_MODE, NearCacheMode.class, NearCacheMode.DISABLED, true);
   }

   public int getNearCacheMaxEntries() {
      return props.getIntProperty(NEAR_CACHE_MAX_ENTRIES, -1);
   }

   public Pattern getNearCacheNamePattern() {
      return (Pattern) props.get(NEAR_CACHE_NAME_PATTERN);
   }

   /**
    * Is version previous to, and not including, 1.2?
    */
   public static boolean isVersionPre12(Configuration cfg) {
      String version = cfg.version().toString();
      return Objects.equals(version, "1.0") || Objects.equals(version, "1.1");
   }

   public long getTransactionTimeout() {
      return props.getLongProperty(TRANSACTION_TIMEOUT, TransactionConfigurationBuilder.DEFAULT_TIMEOUT);
   }
}
