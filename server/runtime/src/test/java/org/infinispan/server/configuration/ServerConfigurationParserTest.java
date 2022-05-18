package org.infinispan.server.configuration;

import static org.infinispan.server.configuration.security.CredentialStoresConfiguration.resolvePassword;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.Exceptions;
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
import org.infinispan.server.security.ElytronPasswordProviderSupplier;
import org.infinispan.server.security.KeyStoreUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.wildfly.security.auth.server.IdentityCredentials;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.credential.store.CredentialStore;
import org.wildfly.security.credential.store.CredentialStoreException;
import org.wildfly.security.credential.store.impl.KeyStoreCredentialStore;
import org.wildfly.security.password.interfaces.ClearPassword;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerConfigurationParserTest extends AbstractConfigurationParserTest {

   @ClassRule
   public static final JUnitThreadTrackerRule tracker = new JUnitThreadTrackerRule();
   public static final char[] PASSWORD = "password".toCharArray();
   public static final char[] SECRET = "secret".toCharArray();

   @BeforeClass
   public static void setup() throws Exception {
      KeyStoreUtils.generateSelfSignedCertificate(pathToKeystore(), null, PASSWORD, PASSWORD, "server", "localhost");
      KeyStoreUtils.generateEmptyKeyStore(getConfigPath().resolve("ServerConfigurationParserTest-truststore.pfx").toString(), SECRET);
      createCredentialStore(getConfigPath().resolve("ServerConfigurationParserTest-credentials.pfx"), SECRET);
   }

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

   public static String pathToKeystore() {
      Path path = getConfigPath().resolve("ServerConfigurationParserTest-keystore.pfx");
      return path.toString();
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

   static void addCredential(KeyStoreCredentialStore store, String alias, String credential) {
      try {
         store.store(alias, new PasswordCredential(ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, credential.toCharArray())), null);
         store.flush();
      } catch (CredentialStoreException e) {
         throw new RuntimeException(e);
      }
   }

   static KeyStoreCredentialStore newCredentialStore(Path location, char[] secret) {
      Exceptions.unchecked(() -> {
         Files.deleteIfExists(location);
         Files.createDirectories(location.getParent());
      });
      KeyStoreCredentialStore credentialStore = new KeyStoreCredentialStore();
      final Map<String, String> map = new HashMap<>();
      map.put("location", location.toString());
      map.put("create", "true");
      try {
         credentialStore.initialize(
               map,
               new CredentialStore.CredentialSourceProtectionParameter(
                     IdentityCredentials.NONE.withCredential(new PasswordCredential(ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, secret)))),
               ElytronPasswordProviderSupplier.PROVIDERS
         );
         return credentialStore;
      } catch (CredentialStoreException e) {
         throw new RuntimeException(e);
      }
   }

   static void createCredentialStore(Path location, char[] secret) {
      KeyStoreCredentialStore credentialStore = newCredentialStore(location, secret);
      addCredential(credentialStore, "ldap", "strongPassword");
      addCredential(credentialStore, "db", "test");
      addCredential(credentialStore, "keystore", "password");
      addCredential(credentialStore, "oauth2", "1fdca4ec-c416-47e0-867a-3d471af7050f");
      addCredential(credentialStore, "trust", "secret");
   }
}
