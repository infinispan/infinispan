package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.configuration.cache.LockSupportStoreConfigurationChildBuilder;

/**
 * JdbcCacheStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface JdbcCacheStoreConfigurationChildBuilder<S extends AbstractJdbcCacheStoreConfigurationBuilder<?, S>> extends LockSupportStoreConfigurationChildBuilder<S> {

   /**
    * Configures a connection pool to be used by this JDBC Cache Store to handle connections to the database
    */
   PooledConnectionFactoryConfigurationBuilder<S> connectionPool();

   /**
    * Configures a DataSource to be used by this JDBC Cache Store to handle connections to the database
    */
   ManagedConnectionFactoryConfigurationBuilder<S> dataSource();


   /**
    * Configures this JDBC Cache Store to use a single connection to the database
    */
   SimpleConnectionFactoryConfigurationBuilder<S> simpleConnection();

   /**
    * Use the specified {@link ConnectionFactory} to handle connections to the database
    */
   //<C extends ConnectionFactoryConfigurationBuilder<?>> C connectionFactory(Class<C> klass);

}
