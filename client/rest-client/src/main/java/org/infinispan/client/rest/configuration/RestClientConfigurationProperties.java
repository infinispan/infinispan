package org.infinispan.client.rest.configuration;

import java.util.Properties;

import org.infinispan.commons.util.TypedProperties;

/**
 * Encapsulate all config properties here
 *
 * @author Tristan Tarrant
 * @version 10.0
 */
public class RestClientConfigurationProperties {
   public static final String ICR = "infinispan.client.rest.";

   public static final String SERVER_LIST = ICR + "server_list";
   public static final String CONTEXT_PATH = ICR + "context_path";

   public static final String TCP_NO_DELAY = ICR + "tcp_no_delay";
   public static final String TCP_KEEP_ALIVE = ICR + "tcp_keep_alive";
   // Connection properties
   public static final String PROTOCOL = ICR + "protocol";
   public static final String SO_TIMEOUT = ICR + "socket_timeout";
   public static final String CONNECT_TIMEOUT = ICR + "connect_timeout";

   // Encryption properties
   public static final String USE_SSL = ICR + "use_ssl";
   public static final String KEY_STORE_FILE_NAME = ICR + "key_store_file_name";
   public static final String KEY_STORE_TYPE = ICR + "key_store_type";
   public static final String KEY_STORE_PASSWORD = ICR + "key_store_password";
   public static final String SNI_HOST_NAME = ICR + "sni_host_name";
   public static final String KEY_ALIAS = ICR + "key_alias";
   public static final String KEY_STORE_CERTIFICATE_PASSWORD = ICR + "key_store_certificate_password";
   public static final String TRUST_STORE_FILE_NAME = ICR + "trust_store_file_name";
   public static final String TRUST_STORE_PATH = ICR + "trust_store_path";
   public static final String TRUST_STORE_TYPE = ICR + "trust_store_type";
   public static final String TRUST_STORE_PASSWORD = ICR + "trust_store_password";
   public static final String SSL_PROTOCOL = ICR + "ssl_protocol";
   public static final String SSL_CONTEXT = ICR + "ssl_context";
   public static final String TRUST_MANAGERS = ICR + "trust_managers";
   public static final String PROVIDER = ICR + "provider";

   // Authentication properties
   public static final String USE_AUTH = ICR + "use_auth";
   public static final String AUTH_MECHANISM = ICR + "sasl_mechanism";
   public static final String AUTH_CALLBACK_HANDLER = ICR + "auth_callback_handler";
   public static final String AUTH_SERVER_NAME = ICR + "auth_server_name";
   public static final String AUTH_USERNAME = ICR + "auth_username";
   public static final String AUTH_PASSWORD = ICR + "auth_password";
   public static final String AUTH_REALM = ICR + "auth_realm";
   public static final String AUTH_CLIENT_SUBJECT = ICR + "auth_client_subject";


   // defaults
   public static final int DEFAULT_REST_PORT = 11222;
   public static final long DEFAULT_SO_TIMEOUT = 60_000;
   public static final long DEFAULT_CONNECT_TIMEOUT = 60_000;
   public static final int DEFAULT_MAX_RETRIES = 10;
   public static final int DEFAULT_BATCH_SIZE = 10_000;

   private final TypedProperties props;


   public RestClientConfigurationProperties() {
      this.props = new TypedProperties();
   }

   public RestClientConfigurationProperties(String serverList) {
      this();
      setServerList(serverList);
   }

   public RestClientConfigurationProperties(Properties props) {
      this.props = props == null ? new TypedProperties() : TypedProperties.toTypedProperties(props);
   }

   public void setServerList(String serverList) {
      props.setProperty(SERVER_LIST, serverList);
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

   public Properties getProperties() {
      return props;
   }

   public Protocol getProtocol() {
      return Protocol.valueOf(props.getProperty(PROTOCOL, Protocol.HTTP_11.name()));
   }

   public void setProtocol(Protocol protocol) {
      props.setProperty(PROTOCOL, protocol.name());
   }

   public long getSoTimeout() {
      return props.getLongProperty(SO_TIMEOUT, DEFAULT_SO_TIMEOUT);
   }

   public void setSocketTimeout(int socketTimeout) {
      props.setProperty(SO_TIMEOUT, socketTimeout);
   }

   public long getConnectTimeout() {
      return props.getLongProperty(CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
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

   public void setKeyStoreCertificatePassword(String keyStoreCertificatePassword) {
      props.setProperty(KEY_STORE_CERTIFICATE_PASSWORD, keyStoreCertificatePassword);
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

   public String getTrustStorePath() {
      return props.getProperty(TRUST_STORE_PATH);
   }

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
      return props.getProperty(AUTH_MECHANISM);
   }

   public void setSaslMechanism(String saslMechanism) {
      props.setProperty(AUTH_MECHANISM, saslMechanism);
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

   public String getAuthRealm() {
      return props.getProperty(AUTH_REALM);
   }

   public void setAuthRealm(String authRealm) {
      props.setProperty(AUTH_REALM, authRealm);
   }

   public void setAuthServerName(String authServerName) {
      props.setProperty(AUTH_SERVER_NAME, authServerName);
   }

   public String getServerList() {
      return props.getProperty(SERVER_LIST);
   }
}
