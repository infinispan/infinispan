package org.infinispan.persistence.jdbc.common.configuration;


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

   @Override
   public <C extends ConnectionFactoryConfigurationBuilder<?>> C connectionFactory(Class<C> klass) {
      return builder.connectionFactory(klass);
   }

   @Override
   public <C extends ConnectionFactoryConfigurationBuilder<?>> C connectionFactory(C factoryBuilder) {
      return builder.connectionFactory(factoryBuilder);
   }

}
