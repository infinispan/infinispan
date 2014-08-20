package org.infinispan.persistence.remote.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 *
 * ConnectionPoolConfigurationBuilder. Specified connection pooling properties for the HotRod client
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ConnectionPoolConfigurationBuilder extends AbstractRemoteStoreConfigurationChildBuilder<RemoteStoreConfigurationBuilder> implements
      Builder<ConnectionPoolConfiguration> {
   private ExhaustedAction exhaustedAction = ExhaustedAction.WAIT;
   private int maxActive = -1;
   private int maxTotal = -1;
   private int maxIdle = -1;
   private int minIdle = 1;
   private long timeBetweenEvictionRuns = 120000;
   private long minEvictableIdleTime = 1800000;
   private boolean testWhileIdle = true;

   ConnectionPoolConfigurationBuilder(RemoteStoreConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Specifies what happens when asking for a connection from a server's pool, and that pool is
    * exhausted.
    */
   public ConnectionPoolConfigurationBuilder exhaustedAction(ExhaustedAction exhaustedAction) {
      this.exhaustedAction = exhaustedAction;
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
      this.maxActive = maxActive;
      return this;
   }

   /**
    * Sets a global limit on the number persistent connections that can be in circulation within the
    * combined set of servers. When non-positive, there is no limit to the total number of
    * persistent connections in circulation. When maxTotal is exceeded, all connections pools are
    * exhausted. The default setting for this parameter is -1 (no limit).
    */
   public ConnectionPoolConfigurationBuilder maxTotal(int maxTotal) {
      this.maxTotal = maxTotal;
      return this;
   }

   /**
    * Controls the maximum number of idle persistent connections, per server, at any time. When
    * negative, there is no limit to the number of connections that may be idle per server. The
    * default setting for this parameter is -1.
    */
   public ConnectionPoolConfigurationBuilder maxIdle(int maxIdle) {
      this.maxIdle = maxIdle;
      return this;
   }

   /**
    * Sets a target value for the minimum number of idle connections (per server) that should always
    * be available. If this parameter is set to a positive number and timeBetweenEvictionRunsMillis
    * > 0, each time the idle connection eviction thread runs, it will try to create enough idle
    * instances so that there will be minIdle idle instances available for each server. The default
    * setting for this parameter is 1.
    */
   public ConnectionPoolConfigurationBuilder minIdle(int minIdle) {
      this.minIdle = minIdle;
      return this;
   }

   /**
    * Indicates how long the eviction thread should sleep before "runs" of examining idle
    * connections. When non-positive, no eviction thread will be launched. The default setting for
    * this parameter is 2 minutes.
    */
   public ConnectionPoolConfigurationBuilder timeBetweenEvictionRuns(long timeBetweenEvictionRuns) {
      this.timeBetweenEvictionRuns = timeBetweenEvictionRuns;
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
      this.minEvictableIdleTime = minEvictableIdleTime;
      return this;
   }

   /**
    * Indicates whether or not idle connections should be validated by sending an TCP packet to the
    * server, during idle connection eviction runs. Connections that fail to validate will be
    * dropped from the pool. This setting has no effect unless timeBetweenEvictionRunsMillis > 0.
    * The default setting for this parameter is true.
    */
   public ConnectionPoolConfigurationBuilder testWhileIdle(boolean testWhileIdle) {
      this.testWhileIdle = testWhileIdle;
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
      return new ConnectionPoolConfiguration(exhaustedAction, maxActive, maxTotal, maxIdle, minIdle, timeBetweenEvictionRuns, minEvictableIdleTime, testWhileIdle);
   }

   @Override
   public ConnectionPoolConfigurationBuilder read(ConnectionPoolConfiguration template) {
      exhaustedAction = template.exhaustedAction();
      maxActive = template.maxActive();
      maxTotal = template.maxTotal();
      maxIdle = template.maxIdle();
      minIdle = template.minIdle();
      timeBetweenEvictionRuns = template.timeBetweenEvictionRuns();
      minEvictableIdleTime = template.minEvictableIdleTime();
      testWhileIdle = template.testWhileIdle();
      return this;
   }
}
