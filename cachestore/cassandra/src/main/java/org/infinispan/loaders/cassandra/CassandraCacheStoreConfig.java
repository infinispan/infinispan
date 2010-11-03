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
	
	/**
	 * @configRef desc="Whether the keySpace is shared between multiple caches"
	 */
	boolean sharedKeyspace = false;
	
	String readConsistencyLevel = "ONE";
	
	String writeConsistencyLevel = "ONE";

	protected PoolProperties poolProperties;

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

	public boolean isSharedKeyspace() {
		return sharedKeyspace;
	}

	public void setSharedKeyspace(boolean sharedKeyspace) {
		this.sharedKeyspace = sharedKeyspace;
	}

	public String getReadConsistencyLevel() {
		return readConsistencyLevel;
	}

	public void setReadConsistencyLevel(String readConsistencyLevel) {
		this.readConsistencyLevel = readConsistencyLevel;
	}

	public String getWriteConsistencyLevel() {
		return writeConsistencyLevel;
	}

	public void setWriteConsistencyLevel(String writeConsistencyLevel) {
		this.writeConsistencyLevel = writeConsistencyLevel;
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

	public boolean isFramed() {
		return poolProperties.isFramed();
	}

	public String getPassword() {
		return poolProperties.getPassword();
	}

	public String getUsername() {
		return poolProperties.getUsername();
	}

	public void setFramed(boolean framed) {
		poolProperties.setFramed(framed);

	}

	public void setPassword(String password) {
		poolProperties.setPassword(password);
	}

	public void setUsername(String username) {
		poolProperties.setUsername(username);
	}
	
	public void setDatasourceJndiLocation(String location) {
		poolProperties.setDataSourceJNDI(location);
	}
	
	public String getDatasourceJndiLocation() {
		return poolProperties.getDataSourceJNDI();
	}

}
