package org.infinispan.cdc.internal.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.infinispan.cdc.configuration.ChangeDataCaptureConfiguration;
import org.infinispan.cdc.configuration.ChangeDataCaptureConfigurationBuilder;
import org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor;
import org.infinispan.cdc.internal.url.DatabaseURL;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ConnectionParametersTest {

   @ParameterizedTest
   @MethodSource("produceConnectionFactories")
   public void testCreateConnectionParameters(ConnectionFactory factory) throws SQLException {
      ConnectionParameters parameters = ConnectionParameters.create(factory);

      assertThat(parameters.username()).isEqualTo("admin");
      assertThat(parameters.password()).isEqualTo("password");
      assertThat(parameters.url().vendor()).isEqualTo(DatabaseVendor.MYSQL);

      DatabaseURL url = parameters.url();
      assertThat(url.host()).isEqualTo("localhost");
      assertThat(url.port()).isEqualTo("3306");
      assertThat(url.schema()).isEqualTo("cdc");
   }

   private static Stream<ConnectionFactory> produceConnectionFactories() {
      return Stream.of(
            createFactory(b -> b.simpleConnection()
                  .driverClass(com.mysql.cj.jdbc.Driver.class)
                  .connectionUrl("jdbc:mysql://localhost:3306/cdc")
                  .username("admin")
                  .password("password")),
            createFactory(b -> b.connectionPool()
                  .driverClass(com.mysql.cj.jdbc.Driver.class)
                  .connectionUrl("jdbc:mysql://localhost:3306/cdc")
                  .username("admin")
                  .password("password")),
            createFactory(b -> b.connectionPool()
                  .propertyFile("src/test/resources/configuration/agroal.properties"))
      );
   }

   private static ConnectionFactory createFactory(Consumer<ChangeDataCaptureConfigurationBuilder> decorator) {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      ChangeDataCaptureConfigurationBuilder builder = cb.addModule(ChangeDataCaptureConfigurationBuilder.class);

      decorator.accept(builder);
      ChangeDataCaptureConfiguration configuration = builder.create();
      return createFactory(configuration.connectionFactory());
   }

   private static ConnectionFactory createFactory(ConnectionFactoryConfiguration cfc) {
      ConnectionFactory factory = ConnectionFactory.getConnectionFactory(cfc.connectionFactoryClass());
      factory.start(cfc, cfc.getClass().getClassLoader());
      return factory;
   }
}
