package org.infinispan.server.configuration;

import static org.infinispan.server.configuration.security.CredentialStoresConfiguration.resolvePassword;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.junit.JUnitThreadTrackerRule;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.configuration.security.LdapRealmConfiguration;
import org.infinispan.server.configuration.security.RealmConfiguration;
import org.infinispan.server.configuration.security.RealmProvider;
import org.infinispan.server.configuration.security.TokenRealmConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.server.router.configuration.SinglePortRouterConfiguration;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerConfigurationParserTest extends AbstractConfigurationParserTest {

   @ClassRule
   public static final JUnitThreadTrackerRule tracker = new JUnitThreadTrackerRule();

   public ServerConfigurationParserTest(MediaType type) {
      super(type);
   }

   @Override
   public String path() {
      return "configuration/" + getClass().getSimpleName() + "." + type.getSubType().toLowerCase(Locale.ROOT);
   }

   @Test
   public void testParser() throws IOException {
      ServerConfiguration serverConfiguration = loadAndParseConfiguration();
      validateConfiguration(serverConfiguration);
   }

   private void validateConfiguration(ServerConfiguration configuration) {
      // Interfaces
      assertEquals(2, configuration.networkInterfaces().size());
      NetworkAddress defaultInterface = configuration.networkInterfaces().get("default").getNetworkAddress();
      assertNotNull(defaultInterface);
      assertTrue(defaultInterface.getAddress().isLoopbackAddress());

      // Socket bindings
      Map<String, SocketBindingConfiguration> socketBindings = configuration.socketBindings();
      assertEquals(5, socketBindings.size());
      assertEquals(11221, socketBindings.get("memcached").port());
      assertEquals(12221, socketBindings.get("memcached-2").port());
      assertEquals(11222, socketBindings.get("default").port());
      assertEquals(11223, socketBindings.get("hotrod").port());
      assertEquals(8080, socketBindings.get("rest").port());

      // Security realms
      Map<String, RealmConfiguration> realms = configuration.security().realms().realms();
      assertEquals(3, realms.size());
      RealmConfiguration realmConfiguration = realms.get("default");
      assertEquals("default", realmConfiguration.name());

      realmConfiguration = realms.get("using-credentials");
      assertEquals("using-credentials", realmConfiguration.name());

      // Data Sources
      Map<String, DataSourceConfiguration> dataSources = configuration.dataSources();
      assertEquals(2, dataSources.size());
      DataSourceConfiguration dataSource = dataSources.get("database");
      assertEquals("jdbc/database", dataSource.jndiName());
      assertEquals("jdbc:h2:tcp://${org.infinispan.test.host.address}:1521/test", dataSource.url());
      assertEquals("test", dataSource.username());
      assertEquals("test", new String(dataSource.password()));
      assertEquals("SELECT 1", dataSource.initialSql());
      assertEquals("org.h2.Driver", dataSource.driver());
      assertEquals(10, dataSource.maxSize());
      assertEquals(1, dataSource.minSize());
      assertEquals(1, dataSource.initialSize());
      assertEquals(1, dataSource.connectionProperties().size());
      assertEquals(10000, dataSource.leakDetection());
      assertEquals(1000, dataSource.backgroundValidation());
      assertEquals(500, dataSource.validateOnAcquisition());
      assertEquals("somevalue", dataSource.connectionProperties().get("someproperty"));
      dataSource = dataSources.get("database-with-credential");
      assertEquals("test", new String(dataSource.password()));

      // Connectors
      List<ProtocolServerConfiguration> connectors = configuration.endpoints().endpoints().get(0).connectors();
      assertEquals(3, connectors.size());
      assertTrue(connectors.get(0) instanceof HotRodServerConfiguration);
      assertTrue(connectors.get(1) instanceof RestServerConfiguration);
      assertTrue(connectors.get(2) instanceof MemcachedServerConfiguration);

      // Ensure endpoints are bound to the interfaces
      SinglePortRouterConfiguration singlePortRouter = configuration.endpoints().endpoints().get(0).singlePortRouter();
      assertEquals(socketBindings.get("default").interfaceConfiguration().getNetworkAddress().getAddress().getHostAddress(), singlePortRouter.host());
      assertEquals(socketBindings.get("default").port(), singlePortRouter.port());
      assertEquals(socketBindings.get("memcached").port(), configuration.endpoints().endpoints().get(0).connectors().get(2).port());

      assertEquals("strongPassword", new String(resolvePassword(realmProvider(realmConfiguration, LdapRealmConfiguration.class).attributes().attribute(Attribute.CREDENTIAL))));
      assertEquals("secret", new String(resolvePassword(realmConfiguration.serverIdentitiesConfiguration().sslConfiguration().trustStore().attributes().attribute(Attribute.PASSWORD))));
      assertEquals("1fdca4ec-c416-47e0-867a-3d471af7050f", new String(resolvePassword(realmProvider(realmConfiguration, TokenRealmConfiguration.class).oauth2Configuration().attributes().attribute(Attribute.CLIENT_SECRET))));

      assertEquals(3, configuration.security.credentialStores().credentialStores().size());
   }

   <T extends RealmProvider> T realmProvider(RealmConfiguration realm, Class<T> providerClass) {
      return (T) realm.realmProviders().stream().filter(r -> providerClass.isAssignableFrom(r.getClass())).findFirst().get();
   }
}
