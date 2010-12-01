package org.infinispan.loaders.cassandra;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import net.dataforte.cassandra.pool.PoolProperties;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.cassandra.keymapper.DefaultTwoWayKey2StringMapper;
import org.infinispan.util.FileLookup;

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

	/**
	 * @configRef desc="Which Cassandra consistency level to use when reading"
	 */
	String readConsistencyLevel = "ONE";

	/**
	 * @configRef desc="Which Cassandra consistency level to use when writing"
	 */
	String writeConsistencyLevel = "ONE";

	/**
	 * @configRef desc=
	 *            "An optional properties file for configuring the underlying cassandra connection pool"
	 */
	String configurationPropertiesFile;
	
	/**
	 * @configRef desc=
	 *            "The keymapper for converting keys to strings (uses the DefaultTwoWayKey2Stringmapper by default"
	 */
	String keyMapper = DefaultTwoWayKey2StringMapper.class.getName();

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

	public String getConfigurationPropertiesFile() {
		return configurationPropertiesFile;
	}

	public void setConfigurationPropertiesFile(String configurationPropertiesFile) throws CacheLoaderException {
		this.configurationPropertiesFile = configurationPropertiesFile;
		readConfigurationProperties();
	}

	private void readConfigurationProperties() throws CacheLoaderException {
		if (configurationPropertiesFile == null || configurationPropertiesFile.trim().length() == 0)
			return;
		InputStream i = new FileLookup().lookupFile(configurationPropertiesFile);
		if (i != null) {
			Properties p = new Properties();
			try {
				p.load(i);
			} catch (IOException ioe) {
				throw new CacheLoaderException("Unable to read environment properties file " + configurationPropertiesFile, ioe);
			}
			// Apply all properties to the PoolProperties object
			for(String propertyName : p.stringPropertyNames()) {
				poolProperties.set(propertyName, p.getProperty(propertyName));
			}
		}
	}

	public String getKeyMapper() {
		return keyMapper;
	}

	public void setKeyMapper(String keyMapper) {
		this.keyMapper = keyMapper;
	}
}
