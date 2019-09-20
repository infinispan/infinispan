package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.ConnectionPoolConfiguration.EXHAUSTED_ACTION;
import static org.infinispan.persistence.remote.configuration.ConnectionPoolConfiguration.MAX_ACTIVE;
import static org.infinispan.persistence.remote.configuration.ConnectionPoolConfiguration.MAX_PENDING_REQUESTS;
import static org.infinispan.persistence.remote.configuration.ConnectionPoolConfiguration.MAX_WAIT;
import static org.infinispan.persistence.remote.configuration.ConnectionPoolConfiguration.MIN_EVICTABLE_IDLE_TIME;
import static org.infinispan.persistence.remote.configuration.ConnectionPoolConfiguration.MIN_IDLE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * ConnectionPoolConfigurationBuilder. Specifies connection pooling properties for the HotRod client.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ConnectionPoolConfigurationBuilder extends AbstractRemoteStoreConfigurationChildBuilder<RemoteStoreConfigurationBuilder> implements
      Builder<ConnectionPoolConfiguration>, ConfigurationBuilderInfo {

   ConnectionPoolConfigurationBuilder(RemoteStoreConfigurationBuilder builder) {
      super(builder, ConnectionPoolConfiguration.attributeDefinitionSet());
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ConnectionPoolConfiguration.ELEMENT_DEFINITION;
   }

   /**
    * Specifies what happens when asking for a connection from a server's pool, and that pool is
    * exhausted.
    */
   public ConnectionPoolConfigurationBuilder exhaustedAction(ExhaustedAction exhaustedAction) {
      this.attributes.attribute(EXHAUSTED_ACTION).set(exhaustedAction);
      return this;
   }

   /**
    * Controls the maximum number of connections per server that are allocated (checked out to
    * client threads, or idle in the pool) at one time. When non-positive, there is no limit to the
    * number of connections per server. When maxActive is reached, the connection pool for that
    * server is said to be exhausted. The default setting for this parameter is -1, i.e. there is no
    * limit.
    */
   public ConnectionPoolConfigurationBuilder maxActive(int maxActive) {
      this.attributes.attribute(MAX_ACTIVE).set(maxActive);
      return this;
   }

   /**
    * Sets a global limit on the number persistent connections that can be in circulation within the
    * combined set of servers. When non-positive, there is no limit to the total number of
    * persistent connections in circulation. When maxTotal is exceeded, all connections pools are
    * exhausted. The default setting for this parameter is -1 (no limit).
    *
    * @deprecated since 10.0. This method has no effect
    */
   @Deprecated
   public ConnectionPoolConfigurationBuilder maxTotal(int maxTotal) {
      return this;
   }

   /**
    * Controls the maximum number of idle persistent connections, per server, at any time. When
    * negative, there is no limit to the number of connections that may be idle per server. The
    * default setting for this parameter is -1.
    *
    * @deprecated since 10.0. This method has no effect
    */
   @Deprecated
   public ConnectionPoolConfigurationBuilder maxIdle(int maxIdle) {
      return this;
   }

   /**
    * The amount of time in milliseconds to wait for a connection to become available when the
    * exhausted action is {@link org.infinispan.client.hotrod.configuration.ExhaustedAction#WAIT}, after which a {@link java.util.NoSuchElementException}
    * will be thrown. If a negative value is supplied, the pool will block indefinitely.
    */
   public ConnectionPoolConfigurationBuilder maxWait(int maxWait) {
      this.attributes.attribute(MAX_WAIT).set(maxWait);
      return this;
   }

   /**
    * Sets a target value for the minimum number of idle connections (per server) that should always
    * be available. If this parameter is set to a positive number and timeBetweenEvictionRunsMillis
    * &gt; 0, each time the idle connection eviction thread runs, it will try to create enough idle
    * instances so that there will be minIdle idle instances available for each server. The default
    * setting for this parameter is 1.
    */
   public ConnectionPoolConfigurationBuilder minIdle(int minIdle) {
      this.attributes.attribute(MIN_IDLE).set(minIdle);
      return this;
   }

   /**
    * Indicates how long the eviction thread should sleep before "runs" of examining idle
    * connections. When non-positive, no eviction thread will be launched. The default setting for
    * this parameter is 2 minutes.
    *
    * @deprecated since 10.0. This method has no effect
    */
   @Deprecated
   public ConnectionPoolConfigurationBuilder timeBetweenEvictionRuns(long timeBetweenEvictionRuns) {
      return this;
   }

   /**
    * Specifies the minimum amount of time that an connection may sit idle in the pool before it is
    * eligible for eviction due to idle time. When non-positive, no connection will be dropped from
    * the pool due to idle time alone. This setting has no effect unless
    * timeBetweenEvictionRunsMillis > 0. The default setting for this parameter is 1800000(30
    * minutes).
    */
   public ConnectionPoolConfigurationBuilder minEvictableIdleTime(long minEvictableIdleTime) {
      this.attributes.attribute(MIN_EVICTABLE_IDLE_TIME).set(minEvictableIdleTime);
      return this;
   }

   /**
    * Indicates whether or not idle connections should be validated by sending an TCP packet to the
    * server, during idle connection eviction runs. Connections that fail to validate will be
    * dropped from the pool. This setting has no effect unless timeBetweenEvictionRunsMillis > 0.
    * The default setting for this parameter is true.
    *
    * @deprecated since 10.0. This method has no effect
    */
   @Deprecated
   public ConnectionPoolConfigurationBuilder testWhileIdle(boolean testWhileIdle) {
      return this;
   }

   /**
    * Specifies maximum number of requests sent over single connection at one instant.
    * Connections with more concurrent requests will be ignored in the pool when choosing available connection
    * and the pool will try to create a new connection if all connections are utilized. Only if the new connection
    * cannot be created and the {@link #exhaustedAction(ExhaustedAction) exhausted action}
    * is set to {@link org.infinispan.client.hotrod.configuration.ExhaustedAction#WAIT} the pool will allow sending the request over one of the over-utilized
    * connections.
    * The rule of thumb is that this should be set to higher values if the values are small (&lt; 1kB) and to lower values
    * if the entries are big (&gt; 10kB).
    * Default setting for this parameter is 5.
    */
   public ConnectionPoolConfigurationBuilder maxPendingRequests(int maxPendingRequests) {
      this.attributes.attribute(MAX_PENDING_REQUESTS).set(maxPendingRequests);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public ConnectionPoolConfiguration create() {
      return new ConnectionPoolConfiguration(attributes.protect());
   }

   @Override
   public ConnectionPoolConfigurationBuilder read(ConnectionPoolConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }
}
