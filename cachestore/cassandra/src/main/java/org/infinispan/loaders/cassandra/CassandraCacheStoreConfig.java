package org.infinispan.loaders.cassandra;

import net.dataforte.cassandra.pool.PoolProperties;

import org.infinispan.loaders.LockSupportCacheStoreConfig;

/**
 * Configures {@link CassandraCacheStore}.
 */
public class CassandraCacheStoreConfig extends LockSupportCacheStoreConfig {
	
	

	/**
	 * @configRef desc="The Cassandra keyspace"
	 */
	String keySpace = "Infinispan";

	/**
	 * @configRef desc="The Cassandra column family for entries"
	 */
	String entryColumnFamily = "InfinispanEntries";

	/**
	 * @configRef desc="The Cassandra column family for expirations"
	 */
	String expirationColumnFamily = "InfinispanExpiration";

	PoolProperties poolProperties;

	public CassandraCacheStoreConfig() {
		setCacheLoaderClassName(CassandraCacheStore.class.getName());
		poolProperties = new PoolProperties();
	}

	public String getKeySpace() {
		return keySpace;
	}

	public void setKeySpace(String keySpace) {
		this.keySpace = keySpace;
	}

	public String getEntryColumnFamily() {
		return entryColumnFamily;
	}

	public void setEntryColumnFamily(String entryColumnFamily) {
		this.entryColumnFamily = entryColumnFamily;
	}

	public String getExpirationColumnFamily() {
		return expirationColumnFamily;
	}

	public void setExpirationColumnFamily(String expirationColumnFamily) {
		this.expirationColumnFamily = expirationColumnFamily;
	}

	public PoolProperties getPoolProperties() {
		return poolProperties;
	}

	public void setHost(String host) {
		poolProperties.setHost(host);
	}

	public String getHost() {
		return poolProperties.getHost();
	}

	public void setPort(int port) {
		poolProperties.setPort(port);
	}

	public int getPort() {
		return poolProperties.getPort();
	}

	
	public int getAbandonWhenPercentageFull() {
		return poolProperties.getAbandonWhenPercentageFull();
	}

	
	public boolean isFramed() {
		return poolProperties.isFramed();
	}

	
	public int getInitialSize() {
		return poolProperties.getInitialSize();
	}

	
	public int getMaxActive() {
		return poolProperties.getMaxActive();
	}

	
	public long getMaxAge() {
		return poolProperties.getMaxAge();
	}

	
	public int getMaxIdle() {
		return poolProperties.getMaxIdle();
	}

	
	public int getMaxWait() {
		return poolProperties.getMaxWait();
	}

	
	public int getMinEvictableIdleTimeMillis() {
		return poolProperties.getMinEvictableIdleTimeMillis();
	}

	
	public int getMinIdle() {
		return poolProperties.getMinIdle();
	}

	
	public String getName() {
		return poolProperties.getName();
	}

	
	public int getNumTestsPerEvictionRun() {
		return poolProperties.getNumTestsPerEvictionRun();
	}

	
	public String getPassword() {
		return poolProperties.getPassword();
	}
	
	public int getRemoveAbandonedTimeout() {
		return poolProperties.getRemoveAbandonedTimeout();
	}

	
	public int getSuspectTimeout() {
		return poolProperties.getSuspectTimeout();
	}

	
	public int getTimeBetweenEvictionRunsMillis() {
		return poolProperties.getTimeBetweenEvictionRunsMillis();
	}

	
	public boolean getUseLock() {
		return poolProperties.getUseLock();
	}

	
	public String getUsername() {
		return poolProperties.getUsername();
	}

	
	public long getValidationInterval() {
		return poolProperties.getValidationInterval();
	}
	
	public boolean isFairQueue() {
		return poolProperties.isFairQueue();
	}

	
	public boolean isJmxEnabled() {
		return poolProperties.isJmxEnabled();
	}

	
	public boolean isLogAbandoned() {
		return poolProperties.isLogAbandoned();
	}

	
	public boolean isRemoveAbandoned() {
		return poolProperties.isRemoveAbandoned();
	}

	
	public boolean isTestOnBorrow() {
		return poolProperties.isTestOnBorrow();
	}

	
	public boolean isTestOnConnect() {
		return poolProperties.isTestOnConnect();
	}

	
	public boolean isTestOnReturn() {
		return poolProperties.isTestOnReturn();
	}

	
	public boolean isTestWhileIdle() {
		return poolProperties.isTestWhileIdle();
	}

	
	public void setAbandonWhenPercentageFull(int percentage) {
		poolProperties.setAbandonWhenPercentageFull(percentage);

	}
	
	public void setFairQueue(boolean fairQueue) {
		poolProperties.setFairQueue(fairQueue);

	}

	
	public void setFramed(boolean framed) {
		poolProperties.setFramed(framed);

	}

	
	public void setInitialSize(int initialSize) {
		poolProperties.setInitialSize(initialSize);

	}

	
	public void setJmxEnabled(boolean jmxEnabled) {
		poolProperties.setJmxEnabled(jmxEnabled);
	}

	
	public void setLogAbandoned(boolean logAbandoned) {
		poolProperties.setLogAbandoned(logAbandoned);
	}

	
	public void setMaxActive(int maxActive) {
		poolProperties.setMaxActive(maxActive);

	}

	
	public void setMaxAge(long maxAge) {
		poolProperties.setMaxAge(maxAge);

	}

	
	public void setMaxIdle(int maxIdle) {
		poolProperties.setMaxIdle(maxIdle);

	}

	
	public void setMaxWait(int maxWait) {
		poolProperties.setMaxWait(maxWait);

	}

	
	public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
		poolProperties.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);

	}

	public void setMinIdle(int minIdle) {
		poolProperties.setMinIdle(minIdle);

	}

	public void setName(String name) {
		poolProperties.setName(name);
	}

	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		poolProperties.setNumTestsPerEvictionRun(numTestsPerEvictionRun);

	}

	public void setPassword(String password) {
		poolProperties.setPassword(password);
	}

	public void setRemoveAbandoned(boolean removeAbandoned) {
		poolProperties.setRemoveAbandoned(removeAbandoned);
	}

	
	public void setRemoveAbandonedTimeout(int removeAbandonedTimeout) {
		poolProperties.setRemoveAbandonedTimeout(removeAbandonedTimeout);

	}

	public void setSuspectTimeout(int seconds) {
		poolProperties.setSuspectTimeout(seconds);

	}

	public void setTestOnBorrow(boolean testOnBorrow) {
		poolProperties.setTestOnBorrow(testOnBorrow);

	}

	public void setTestOnConnect(boolean testOnConnect) {
		poolProperties.setTestOnConnect(testOnConnect);

	}

	public void setTestOnReturn(boolean testOnReturn) {
		poolProperties.setTestOnReturn(testOnReturn);
	}

	public void setTestWhileIdle(boolean testWhileIdle) {
		poolProperties.setTestWhileIdle(testWhileIdle);
	}

	public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
		poolProperties.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);

	}

	public void setUsername(String username) {
		poolProperties.setUsername(username);
	}

	public void setValidationInterval(long validationInterval) {
		poolProperties.setValidationInterval(validationInterval);
	}
}
