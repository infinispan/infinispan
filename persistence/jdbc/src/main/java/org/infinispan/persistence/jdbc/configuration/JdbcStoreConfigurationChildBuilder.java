package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.configuration.cache.LoaderConfigurationChildBuilder;

/**
 * JdbcStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface JdbcStoreConfigurationChildBuilder<S extends AbstractJdbcStoreConfigurationBuilder<?, S>> extends LoaderConfigurationChildBuilder<S> {

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
    * Use the specified ConnectionFactory to handle connection to the database
    */
   <C extends ConnectionFactoryConfigurationBuilder<?>> C connectionFactory(Class<C> klass);

   /**
    * Use the specified {@link ConnectionFactoryConfigurationBuilder} to configure connections to
    * the database
    */
   <C extends ConnectionFactoryConfigurationBuilder<?>> C connectionFactory(C builder);
}
