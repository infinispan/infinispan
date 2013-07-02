package org.infinispan.loaders.cassandra.configuration;

import java.util.Collections;
import java.util.List;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.cassandra.CassandraCacheStoreConfig;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.TypedProperties;

@BuiltBy(CassandraCacheStoreConfigurationBuilder.class)
public class CassandraCacheStoreConfiguration extends AbstractStoreConfiguration implements
      LegacyLoaderAdapter<CassandraCacheStoreConfig> {

   private final boolean autoCreateKeyspace;
   private final String configurationPropertiesFile;
   private final String entryColumnFamily;
   private final String expirationColumnFamily;
   private final boolean framed;
   private final List<CassandraServerConfiguration> servers;
   private final String keyMapper;
   private final String keySpace;
   private final String password;
   private final ConsistencyLevel readConsistencyLevel;
   private final boolean sharedKeyspace;
   private final String username;
   private final ConsistencyLevel writeConsistencyLevel;

   public CassandraCacheStoreConfiguration(boolean autoCreateKeyspace, String configurationPropertiesFile,
         String entryColumnFamily, String expirationColumnFamily, boolean framed, List<CassandraServerConfiguration> servers, String keyMapper,
         String keySpace, String password, boolean sharedKeyspace, String username,
         ConsistencyLevel readConsistencyLevel, ConsistencyLevel writeConsistencyLevel, boolean purgeOnStartup,
         boolean purgeSynchronously, int purgerThreads, boolean fetchPersistentState, boolean ignoreModifications,
         TypedProperties properties, AsyncStoreConfiguration asyncStoreConfiguration,
         SingletonStoreConfiguration singletonStoreConfiguration) {
      super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, properties,
            asyncStoreConfiguration, singletonStoreConfiguration);
      this.autoCreateKeyspace = autoCreateKeyspace;
      this.configurationPropertiesFile = configurationPropertiesFile;
      this.entryColumnFamily = entryColumnFamily;
      this.expirationColumnFamily = expirationColumnFamily;
      this.framed = framed;
      this.servers = Collections.unmodifiableList(servers);
      this.keyMapper = keyMapper;
      this.keySpace = keySpace;
      this.password = password;
      this.sharedKeyspace = sharedKeyspace;
      this.username = username;
      this.readConsistencyLevel = readConsistencyLevel;
      this.writeConsistencyLevel = writeConsistencyLevel;
   }

   public boolean autoCreateKeyspace() {
      return autoCreateKeyspace;
   }

   public String configurationPropertiesFile() {
      return configurationPropertiesFile;
   }

   public String entryColumnFamily() {
      return entryColumnFamily;
   }

   public String expirationColumnFamily() {
      return expirationColumnFamily;
   }

   public boolean framed() {
      return framed;
   }

   public String keyMapper() {
      return keyMapper;
   }

   public List<CassandraServerConfiguration> hosts() {
      return servers;
   }

   public String keySpace() {
      return keySpace;
   }

   public List<CassandraServerConfiguration> servers() {
      return servers;
   }


   public boolean sharedKeyspace() {
      return sharedKeyspace;
   }

   public String password() {
      return password;
   }

   public String username() {
      return username;
   }

   public ConsistencyLevel readConsistencyLevel() {
      return readConsistencyLevel;
   }

   public ConsistencyLevel writeConsistencyLevel() {
      return writeConsistencyLevel;
   }

   @Override
   public CassandraCacheStoreConfig adapt() {
      CassandraCacheStoreConfig config = new CassandraCacheStoreConfig();

      LegacyConfigurationAdaptor.adapt(this, config);

      config.setAutoCreateKeyspace(autoCreateKeyspace);
      config.setConfigurationPropertiesFile(configurationPropertiesFile);
      config.setEntryColumnFamily(entryColumnFamily);
      config.setExpirationColumnFamily(expirationColumnFamily);
      config.setFramed(framed);
      StringBuilder host = new StringBuilder();
      for (CassandraServerConfiguration s : this.servers) {
         if (host.length() > 0)
            host.append(";");
         host.append(s.host());
      }
      config.setHost(host.toString());
      config.setKeyMapper(keyMapper);
      config.setKeySpace(keySpace);
      config.setPassword(password);
      config.setReadConsistencyLevel(readConsistencyLevel);
      config.setSharedKeyspace(sharedKeyspace);
      config.setUsername(username);
      config.setWriteConsistencyLevel(writeConsistencyLevel);

      return config;
   }


}
