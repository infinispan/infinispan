package org.infinispan.persistence.jdbc.configuration;


import org.infinispan.configuration.cache.AbstractStoreConfigurationChildBuilder;

/**
 * AbstractJdbcStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractJdbcStoreConfigurationChildBuilder<S extends AbstractJdbcStoreConfigurationBuilder<?, S>>
      extends AbstractStoreConfigurationChildBuilder<S> implements JdbcStoreConfigurationChildBuilder<S> {

   private AbstractJdbcStoreConfigurationBuilder<?, S> builder;

   protected AbstractJdbcStoreConfigurationChildBuilder(AbstractJdbcStoreConfigurationBuilder<?, S> builder) {
      super(builder);
      this.builder = builder;
   }

   @Override
   public PooledConnectionFactoryConfigurationBuilder<S> connectionPool() {
      return builder.connectionPool();
   }

   @Override
   public ManagedConnectionFactoryConfigurationBuilder<S> dataSource() {
      return builder.dataSource();
   }

   @Override
   public SimpleConnectionFactoryConfigurationBuilder<S> simpleConnection() {
      return builder.simpleConnection();
   }

}
