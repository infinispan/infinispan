package org.infinispan.hotrod.configuration;

import static org.infinispan.hotrod.configuration.ConnectionPoolConfiguration.EXHAUSTED_ACTION;
import static org.infinispan.hotrod.configuration.ConnectionPoolConfiguration.MAX_ACTIVE;
import static org.infinispan.hotrod.configuration.ConnectionPoolConfiguration.MAX_PENDING_REQUESTS;
import static org.infinispan.hotrod.configuration.ConnectionPoolConfiguration.MAX_WAIT;
import static org.infinispan.hotrod.configuration.ConnectionPoolConfiguration.MIN_EVICTABLE_IDLE_TIME;
import static org.infinispan.hotrod.configuration.ConnectionPoolConfiguration.MIN_IDLE;

import java.util.Properties;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;

/**
 * ConnectionPoolConfigurationBuilder. Specifies connection pooling properties for the HotRod client.
 *
 * @since 14.0
 */
public class ConnectionPoolConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ConnectionPoolConfiguration> {
   private final AttributeSet attributes = ConnectionPoolConfiguration.attributeDefinitionSet();

   ConnectionPoolConfigurationBuilder(HotRodConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Specifies what happens when asking for a connection from a server's pool, and that pool is
    * exhausted.
    */
   public ConnectionPoolConfigurationBuilder exhaustedAction(ExhaustedAction exhaustedAction) {
      attributes.attribute(EXHAUSTED_ACTION).set(exhaustedAction);
      return this;
   }

   /**
    * Returns the configured action when the pool has become exhausted.
    * @return the action to perform
    */
   public ExhaustedAction exhaustedAction() {
      return attributes.attribute(EXHAUSTED_ACTION).get();
   }

   /**
    * Controls the maximum number of connections per server that are allocated (checked out to
    * client threads, or idle in the pool) at one time. When non-positive, there is no limit to the
    * number of connections per server. When maxActive is reached, the connection pool for that
    * server is said to be exhausted. The default setting for this parameter is -1, i.e. there is no
    * limit.
    */
   public ConnectionPoolConfigurationBuilder maxActive(int maxActive) {
      attributes.attribute(MAX_ACTIVE).set(maxActive);
      return this;
   }

   /**
    * Returns the number of configured maximum connections per server that can be allocated. When this is non-positive
    * there is no limit to the number of connections.
    * @return maximum number of open connections to a server
    */
   public int maxActive() {
      return attributes.attribute(MAX_ACTIVE).get();
   }

   /**
    * The amount of time in milliseconds to wait for a connection to become available when the
    * exhausted action is {@link ExhaustedAction#WAIT}, after which a {@link java.util.NoSuchElementException}
    * will be thrown. If a negative value is supplied, the pool will block indefinitely.
    */
   public ConnectionPoolConfigurationBuilder maxWait(long maxWait) {
      attributes.attribute(MAX_WAIT).set(maxWait);
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
      attributes.attribute(MIN_IDLE).set(minIdle);
      return this;
   }

   /**
    * Specifies the minimum amount of time that an connection may sit idle in the pool before it is
    * eligible for eviction due to idle time. When non-positive, no connection will be dropped from
    * the pool due to idle time alone. This setting has no effect unless
    * timeBetweenEvictionRunsMillis &gt; 0. The default setting for this parameter is 1800000(30
    * minutes).
    */
   public ConnectionPoolConfigurationBuilder minEvictableIdleTime(long minEvictableIdleTime) {
      attributes.attribute(MIN_EVICTABLE_IDLE_TIME).set(minEvictableIdleTime);
      return this;
   }

   /**
    * Specifies maximum number of requests sent over single connection at one instant.
    * Connections with more concurrent requests will be ignored in the pool when choosing available connection
    * and the pool will try to create a new connection if all connections are utilized. Only if the new connection
    * cannot be created and the {@link #exhaustedAction(ExhaustedAction) exhausted action}
    * is set to {@link ExhaustedAction#WAIT} the pool will allow sending the request over one of the over-utilized
    * connections.
    * The rule of thumb is that this should be set to higher values if the values are small (&lt; 1kB) and to lower values
    * if the entries are big (&gt; 10kB).
    * Default setting for this parameter is 5.
    */
   public ConnectionPoolConfigurationBuilder maxPendingRequests(int maxPendingRequests) {
      attributes.attribute(MAX_PENDING_REQUESTS).set(maxPendingRequests);
      return this;
   }

   /**
    * Configures the connection pool parameter according to properties
    */
   public ConnectionPoolConfigurationBuilder withPoolProperties(Properties properties) {
      attributes.fromProperties(TypedProperties.toTypedProperties(properties), "org.infinispan.client.connection_pool.");
      return this;
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
