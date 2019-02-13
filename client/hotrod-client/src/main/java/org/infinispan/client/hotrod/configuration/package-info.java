/**
 * Hot Rod client configuration API.
 *
 * <p>It is possible to configure the {@link org.infinispan.client.hotrod.RemoteCacheManager} either programmatically,
 * by constructing a {@link org.infinispan.client.hotrod.configuration.Configuration} using a {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder}
 * or declaratively, by placing a properties file named<tt>hotrod-client.properties</tt> in the classpath.</p>
 *
 * <p>The following table describes the individual properties
 * and the related programmatic configuration API.</p>
 *
 * <table cellspacing="0" cellpadding="3" border="0">
 *    <thead>
 *       <tr>
 *          <th>Name</th>
 *          <th>Type</th>
 *          <th>Default</th>
 *          <th>Description</th>
 *       </tr>
 *    </thead>
 *    <tbody>
 *       <tr>
 *          <th colspan="4">Connection properties</th>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.server_list</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>Adds a list of remote servers in the form: host1[:port][;host2[:port]]...</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.tcp_no_delay</b></td>
 *          <td>Boolean</td>
 *          <td>true</td>
 *          <td>Enables/disables the {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#tcpNoDelay(boolean) TCP_NO_DELAY} flag</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.tcp_keep_alive</b></td>
 *          <td>Boolean</td>
 *          <td>false</td>
 *          <td>Enables/disables the {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#tcpKeepAlive(boolean) TCP_KEEPALIVE} flag</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.client_intelligence</b></td>
 *          <td>String</td>
 *          <td>{@link org.infinispan.client.hotrod.configuration.ClientIntelligence#HASH_DISTRIBUTION_AWARE HASH_DISTRIBUTION_AWARE}</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#clientIntelligence(ClientIntelligence) ClientIntelligence}</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.request_balancing_strategy</b></td>
 *          <td>String (class name)</td>
 *          <td>{@link org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy RoundRobinBalancingStrategy}</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#balancingStrategy(String) FailoverRequestBalancingStrategy}</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.socket_timeout</b></td>
 *          <td>Integer</td>
 *          <td>60000</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#socketTimeout(int) timeout} for socket read/writes</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.connect_timeout</b></td>
 *          <td>Integer</td>
 *          <td>60000</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#connectionTimeout(int) timeout} for connections</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.max_retries</b></td>
 *          <td>Integer</td>
 *          <td>10</td>
 *          <td>The maximum number of operation {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#maxRetries(int) retries}</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.batch_size</b></td>
 *          <td>Integer</td>
 *          <td>10000</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#batchSize(int) size} of a batches when iterating</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.protocol_version</b></td>
 *          <td>String</td>
 *          <td>The highest version supported by the client in use</td>
 *          <td>The Hot Rod {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#version(org.infinispan.client.hotrod.ProtocolVersion) version}.</td>
 *       </tr>
 *       <tr>
 *          <th colspan="4">Connection pool properties</th>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.connection_pool.max_active</b></td>
 *          <td>Integer</td>
 *          <td>-1 (no limit)</td>
 *          <td>Maximum number of {@link org.infinispan.client.hotrod.configuration.ConnectionPoolConfigurationBuilder#maxActive(int) connections} per server</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.connection_pool.exhausted_action</b></td>
 *          <td>Integer</td>
 *          <td>{@link org.infinispan.client.hotrod.configuration.ExhaustedAction#WAIT WAIT}</td>
 *          <td>Specifies what happens when asking for a connection from a server's pool, and that pool is {@link org.infinispan.client.hotrod.configuration.ConnectionPoolConfigurationBuilder#exhaustedAction(org.infinispan.client.hotrod.configuration.ExhaustedAction) exhausted}.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.connection_pool.max_wait</b></td>
 *          <td>Long</td>
 *          <td>-1 (no limit)</td>
 *          <td>{@link org.infinispan.client.hotrod.configuration.ConnectionPoolConfigurationBuilder#maxWait(long) Time} to wait in milliseconds for a connection to become available when exhausted_action is WAIT</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.connection_pool.min_idle</b></td>
 *          <td>Integer</td>
 *          <td>-1 (no limit)</td>
 *          <td>Minimum number of idle {@link org.infinispan.client.hotrod.configuration.ConnectionPoolConfigurationBuilder#minIdle(int) connections} (per server) that should always be available</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.connection_pool.min_evictable_idle_time</b></td>
 *          <td>Integer</td>
 *          <td>-1 (no limit)</td>
 *          <td>Minimum amount of {@link org.infinispan.client.hotrod.configuration.ConnectionPoolConfigurationBuilder#minEvictableIdleTime(long) time} that an connection may sit idle in the pool</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.connection_pool.max_pending_requests</b></td>
 *          <td>Integer</td>
 *          <td>-1 (no limit)</td>
 *          <td>Specifies maximum number of {@link org.infinispan.client.hotrod.configuration.ConnectionPoolConfigurationBuilder#maxPendingRequests(int) requests} sent over single connection at one instant.</td>
 *       </tr>
 *       <tr>
 *          <th colspan="4">Thread pool properties</th>
 *       </tr>
 *       <tr>
 *         <td><b>infinispan.client.hotrod.async_executor_factory</b></td>
 *          <td>String (class name)</td>
 *          <td>{@link org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory DefaultAsyncExecutorFactory}</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.ExecutorFactoryConfigurationBuilder#factoryClass(String) factory} for creating threads</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.default_executor_factory.pool_size</b></td>
 *          <td>Integer</td>
 *          <td>99</td>
 *          <td>Size of the thread pool</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.default_executor_factory.threadname_prefix</b></td>
 *          <td>String</td>
 *          <td>HotRod-client-async-pool</td>
 *          <td>Prefix for the default executor thread names</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.default_executor_factory.threadname_suffix</b></td>
 *          <td>String</td>
 *          <td>(empty)</td>
 *          <td>Suffix for the default executor thread names</td>
 *       </tr>
 *       <tr>
 *          <th colspan="4">Marshalling properties</th>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.marshaller</b></td>
 *          <td>String (class name)</td>
 *          <td>{@link org.infinispan.commons.marshall.jboss.GenericJBossMarshaller GenericJBossMarshaller}</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#marshaller(String) marshaller} that serializes keys and values</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.key_size_estimate</b></td>
 *          <td>Integer</td>
 *          <td>64</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#keySizeEstimate(int) estimated&nbsp;size} of keys in bytes when marshalled.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.value_size_estimate</b></td>
 *          <td>Integer</td>
 *          <td>512</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#valueSizeEstimate(int) estimated&nbsp;size} of values in bytes when marshalled.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.force_return_values</b></td>
 *          <td>Boolean</td>
 *          <td>false</td>
 *          <td>Whether to {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#forceReturnValues(boolean) return&nbsp;values} for puts/removes</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.java_serial_whitelist</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>A {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#addJavaSerialWhiteList(String...) class&nbsp;whitelist} which are trusted for unmarshalling.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.hash_function_impl.2</b></td>
 *          <td>String</td>
 *          <td>{@link org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV2 ConsistentHashV2}</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#consistentHashImpl(int, String) hash&nbsp;function} to use.</td>
 *       </tr>
 *       <tr>
 *          <th colspan="4">Encryption (TLS/SSL) properties</th>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.use_ssl</b></td>
 *          <td>Boolean</td>
 *          <td>false</td>
 *          <td>{@link org.infinispan.client.hotrod.configuration.SslConfigurationBuilder#enable() Enable&nbsp;TLS} (implicitly enabled if a trust store is set)</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.key_store_file_name</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.SslConfigurationBuilder#keyStoreFileName(String) filename} of a keystore to use when using client certificate authentication.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.key_store_type</b></td>
 *          <td>String</td>
 *          <td>JKS</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.SslConfigurationBuilder#keyStoreType(String) keystore&nbsp;type}</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.key_store_password</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.SslConfigurationBuilder#keyStorePassword(char[]) keystore&nbsp;password}</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.key_alias</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.SslConfigurationBuilder#keyAlias(String) alias} of the </td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.key_store_certificate_password</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.SslConfigurationBuilder#keyStoreCertificatePassword(char[]) certificate&nbsp;password} in the keystore.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.trust_store_file_name</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.SslConfigurationBuilder#trustStoreFileName(String) path} of the trust store.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.trust_store_path</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.SslConfigurationBuilder#trustStorePath(String) path} of the trust store in PEM format.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.trust_store_type</b></td>
 *          <td>String</td>
 *          <td>JKS</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.SslConfigurationBuilder#trustStoreType(String) type} of the trust store.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.trust_store_password</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.SslConfigurationBuilder#trustStorePassword(char[]) password} of the trust store.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.sni_host_name</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.SslConfigurationBuilder#sniHostName(String) SNI&nbsp;hostname} to connect to.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.ssl_protocol</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.SslConfigurationBuilder#protocol(String) SSL&nbsp;protocol} to use (e.g. TLSv1.2)</td>
 *       </tr>
 *       <tr>
 *          <th colspan="4">Authentication properties</th>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.use_auth</b></td>
 *          <td>Boolean</td>
 *          <td>Implicitly enabled by other authentication properties</td>
 *          <td>{@link org.infinispan.client.hotrod.configuration.AuthenticationConfigurationBuilder#enabled(boolean) Enable} authentication.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.sasl_mechanism</b></td>
 *          <td>String</td>
 *          <td>DIGEST-MD5 if username and password are set<br>EXTERNAL if a key store is set</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.AuthenticationConfigurationBuilder#saslMechanism(String) SASL&nbsp;mechanism} to use for authentication.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.auth_callback_handler</b></td>
 *          <td>String</td>
 *          <td>Chosen automatically based on selected SASL mech</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.AuthenticationConfigurationBuilder#callbackHandler(javax.security.auth.callback.CallbackHandler) CallbackHandler} to use for providing credentials for authentication.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.auth_server_name</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.AuthenticationConfigurationBuilder#serverName(String) server&nbsp;name} to use (for Kerberos).</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.auth_username</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.AuthenticationConfigurationBuilder#username(String) username}</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.auth_password</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.AuthenticationConfigurationBuilder#password(char[]) password}</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.auth_realm</b></td>
 *          <td>String</td>
 *          <td>ApplicationRealm</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.AuthenticationConfigurationBuilder#realm(String) realm} (for DIGEST-MD5 authentication).</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.sasl_properties.*</b></td>
 *          <td>String</td>
 *          <td>N/A</td>
 *          <td>A {@link org.infinispan.client.hotrod.configuration.AuthenticationConfigurationBuilder#saslProperties(java.util.Map) SASL&nbsp;property} (mech-specific)</td>
 *       </tr>
 *       <tr>
 *          <th colspan="4">Transaction properties</th>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.transaction.transaction_manager_lookup</b></td>
 *          <td>String (class name)</td>
 *          <td>{@link org.infinispan.client.hotrod.configuration.TransactionConfigurationBuilder#defaultTransactionManagerLookup() GenericTransactionManagerLookup}</td>
 *          <td>A class to {@link org.infinispan.client.hotrod.configuration.TransactionConfigurationBuilder#transactionManagerLookup(org.infinispan.commons.tx.lookup.TransactionManagerLookup) lookup} available transaction managers.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.transaction.transaction_mode</b></td>
 *          <td>String ({@link org.infinispan.client.hotrod.configuration.TransactionMode} enum name)</td>
 *          <td>{@link org.infinispan.client.hotrod.configuration.TransactionMode#NONE NONE}</td>
 *          <td>The default {@link org.infinispan.client.hotrod.configuration.TransactionConfigurationBuilder#transactionMode(TransactionMode) transaction&nbsp;mode}</td>
 *       </tr>
 *        <tr>
 *          <td><b>infinispan.client.hotrod.transaction.timeout</b></td>
 *          <td>long</td>
 *          <td>60000L</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.TransactionConfigurationBuilder#timeout(long, java.util.concurrent.TimeUnit)} timeout.</td>
 *       </tr>
 *       <tr>
 *          <th colspan="4">Near cache properties</th>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.near_cache.mode</b></td>
 *          <td>String ({@link org.infinispan.client.hotrod.configuration.NearCacheMode} enum name)</td>
 *          <td>{@link org.infinispan.client.hotrod.configuration.NearCacheMode#DISABLED DISABLED}</td>
 *          <td>The default near-cache {@link org.infinispan.client.hotrod.configuration.NearCacheConfigurationBuilder#mode(NearCacheMode) mode}</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.near_cache.max_entries</b></td>
 *          <td>Integer</td>
 *          <td>-1 (no limit)</td>
 *          <td>The {@link org.infinispan.client.hotrod.configuration.NearCacheConfigurationBuilder#maxEntries(int) maximum} number of entries to keep in the local cache.</td>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.near_cache.name_pattern</b></td>
 *          <td>String (regex pattern, see {@link java.util.regex.Pattern})</td>
 *          <td>null (matches all cache names)</td>
 *          <td>A {@link org.infinispan.client.hotrod.configuration.NearCacheConfigurationBuilder#cacheNamePattern(String) regex} which matches caches for which near-caching should be enabled.</td>
 *       </tr>
 *       <tr>
 *          <th colspan="4">XSite properties</th>
 *       </tr>
 *       <tr>
 *          <td><b>infinispan.client.hotrod.cluster.SITE</b></td>
 *          <td>String HOST and int PORT configuration</td>
 *          <td>Example for siteA and siteB:<br/>
 *           infinispan.client.hotrod.cluster.siteA=hostA1:11222; hostA2:11223`<br/>
 *           infinispan.client.hotrod.cluster.siteB=hostB1:11222; hostB2:11223`
 *          </td>
 *          <td>Relates to {@link org.infinispan.client.hotrod.configuration.ClusterConfigurationBuilder#addCluster(java.lang.String)} and
 *          {@link org.infinispan.client.hotrod.configuration.ClusterConfigurationBuilder#addClusterNode(java.lang.String, int)}</td>
 *       </tr>
 *        <tr>
 *           <th colspan="4">Statistics properties</th>
 *        </tr>
 *        <tr>
 *           <td><b>infinispan.client.hotrod.statistics</b></td>
 *           <td>Boolean</td>
 *           <td>Default value {@link org.infinispan.client.hotrod.configuration.StatisticsConfiguration#ENABLED}</td>
 *           <td>Relates to {@link org.infinispan.client.hotrod.configuration.StatisticsConfigurationBuilder#enabled(boolean)}</td>
 *        </tr>
 *        <tr>
 *           <td><b>infinispan.client.hotrod.jmx</b></td>
 *           <td>Boolean</td>
 *           <td>Default value {@link org.infinispan.client.hotrod.configuration.StatisticsConfiguration#JMX_ENABLED}</td>
 *           <td>Relates to {@link org.infinispan.client.hotrod.configuration.StatisticsConfigurationBuilder#jmxEnabled(boolean)}</td>
 *        </tr>
 *        <tr>
 *           <td><b>infinispan.client.hotrod.jmx_name</b></td>
 *           <td>String</td>
 *           <td>Default value {@link org.infinispan.client.hotrod.configuration.StatisticsConfiguration#JMX_NAME}</td>
 *           <td>Relates to {@link org.infinispan.client.hotrod.configuration.StatisticsConfigurationBuilder#jmxName(java.lang.String)}</td>
 *        </tr>
 *        <tr>
 *           <td><b>infinispan.client.hotrod.jmx_domain</b></td>
 *           <td>String</td>
 *           <td>Default value {@link org.infinispan.client.hotrod.configuration.StatisticsConfiguration#JMX_DOMAIN}</td>
 *           <td>Relates to {@link org.infinispan.client.hotrod.configuration.StatisticsConfigurationBuilder#jmxDomain(java.lang.String)}</td>
 *        </tr>
 *    </tbody>
 * </table>
 *
 * @public
 */
package org.infinispan.client.hotrod.configuration;
