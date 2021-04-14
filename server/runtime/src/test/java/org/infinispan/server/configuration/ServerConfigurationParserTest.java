package org.infinispan.server.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.StringBuilderWriter;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.junit.JUnitThreadTrackerRule;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.security.RealmConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.server.network.SocketBinding;
import org.infinispan.server.router.configuration.SinglePortRouterConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.wildfly.security.auth.server.IdentityCredentials;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.credential.store.CredentialStore;
import org.wildfly.security.credential.store.CredentialStoreException;
import org.wildfly.security.credential.store.WildFlyElytronCredentialStoreProvider;
import org.wildfly.security.credential.store.impl.KeyStoreCredentialStore;
import org.wildfly.security.password.interfaces.ClearPassword;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@RunWith(Parameterized.class)
public class ServerConfigurationParserTest {

   @ClassRule
   public static final JUnitThreadTrackerRule tracker = new JUnitThreadTrackerRule();

   private final MediaType type;

   @BeforeClass
   public static void setup() {
      System.setProperty("org.infinispan.configuration.clear-text-secrets", "true");
      registerSecurityProviders();
      createCredentialStore(getConfigPath().resolve("credentials.pfx"), "secret");
   }

   @AfterClass
   public static void cleanup() {
      System.clearProperty("org.infinispan.configuration.clear-text-secrets");
   }

   @Parameterized.Parameters(name = "{0}")
   public static Iterable<MediaType> data() {
      return Arrays.asList(MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.APPLICATION_YAML);
   }

   public ServerConfigurationParserTest(MediaType type) {
      this.type = type;
   }

   @Test
   public void testParser() throws IOException {
      FileLookup fileLookup = FileLookupFactory.newInstance();
      URL url = fileLookup.lookupFileLocation("configuration/" + getClass().getSimpleName() + "." + type.getSubType().toLowerCase(Locale.ROOT), ServerConfigurationParserTest.class.getClassLoader());
      Properties properties = new Properties();
      properties.setProperty(Server.INFINISPAN_SERVER_CONFIG_PATH, getConfigPath().toString());
      properties.setProperty(Server.INFINISPAN_SERVER_HOME_PATH, Paths.get(System.getProperty("build.directory")).toString());
      ParserRegistry registry = new ParserRegistry(this.getClass().getClassLoader(), false, properties);
      ConfigurationBuilderHolder holder = registry.parse(url);
      GlobalConfiguration global = holder.getGlobalConfigurationBuilder().build();
      ServerConfiguration serverConfiguration = global.module(ServerConfiguration.class);
      validateConfiguration(serverConfiguration);

      // Output serialized version
      for(MediaType t : data()) {
         StringBuilderWriter sw = new StringBuilderWriter();
         try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(t).build()) {
            new ServerConfigurationSerializer().serialize(w, serverConfiguration);
         }
         System.out.println("==================== " + t);
         System.out.println(sw);
      }
   }

   private void validateConfiguration(ServerConfiguration configuration) {
      // Interfaces
      assertEquals(2, configuration.networkInterfaces().size());
      NetworkAddress defaultInterface = configuration.networkInterfaces().get("default");
      assertNotNull(defaultInterface);
      assertTrue(defaultInterface.getAddress().isLoopbackAddress());

      // Socket bindings
      Map<String, SocketBinding> socketBindings = configuration.socketBindings();
      assertEquals(5, socketBindings.size());
      assertEquals(11221, socketBindings.get("memcached").getPort());
      assertEquals(12221, socketBindings.get("memcached-2").getPort());
      assertEquals(11222, socketBindings.get("default").getPort());
      assertEquals(11223, socketBindings.get("hotrod").getPort());
      assertEquals(8080, socketBindings.get("rest").getPort());

      // Security realms
      List<RealmConfiguration> realms = configuration.security().realms().realms();
      assertEquals(3, realms.size());
      RealmConfiguration realmConfiguration = realms.get(0);
      assertEquals("default", realmConfiguration.name());

      realmConfiguration = realms.get(1);
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
      assertEquals(socketBindings.get("default").getAddress().getAddress().getHostAddress(), singlePortRouter.host());
      assertEquals(socketBindings.get("default").getPort(), singlePortRouter.port());
      assertEquals(socketBindings.get("memcached").getPort(), configuration.endpoints().endpoints().get(0).connectors().get(2).port());

      assertEquals("strongPassword", new String((char[]) realmConfiguration.ldapConfiguration().attributes().attribute(Attribute.CREDENTIAL).get()));
      assertEquals("secret", new String((char[]) realmConfiguration.serverIdentitiesConfiguration().sslConfiguration().trustStore().attributes().attribute(Attribute.PASSWORD).get())); //stores it as char[]
      assertEquals("1fdca4ec-c416-47e0-867a-3d471af7050f", new String((char[]) realmConfiguration.tokenConfiguration().oauth2Configuration().attributes().attribute(Attribute.CLIENT_SECRET).get()));
      assertEquals("password", new String((char[]) realmConfiguration.serverIdentitiesConfiguration().sslConfiguration().keyStore().attributes().attribute(Attribute.KEYSTORE_PASSWORD).get()));
   }

   public static Path getConfigPath() {
      return Paths.get(System.getProperty("build.directory"), "test-classes", "configuration");
   }

   public static void registerSecurityProviders() {
      WildFlyElytronCredentialStoreProvider provider = WildFlyElytronCredentialStoreProvider.getInstance();
      if (java.security.Security.getProvider(provider.getName()) == null) {
         java.security.Security.insertProviderAt(provider, 1);
      }
   }

   static void addCredential(KeyStoreCredentialStore store, String alias, String credential) {
      try {
         store.store(alias, new PasswordCredential(ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, credential.toCharArray())), null);
         store.flush();
      } catch (CredentialStoreException e) {
         throw new RuntimeException(e);
      }
   }

   static KeyStoreCredentialStore newCredentialStore(Path location, String secret) {
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
                     IdentityCredentials.NONE.withCredential(new PasswordCredential(ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, secret.toCharArray())))),
               null
         );
         return credentialStore;
      } catch (CredentialStoreException e) {
         throw new RuntimeException(e);
      }
   }

   static void createCredentialStore(Path location, String secret) {
      KeyStoreCredentialStore credentialStore = newCredentialStore(location, secret);
      addCredential(credentialStore, "ldap", "strongPassword");
      addCredential(credentialStore, "db", "test");
      addCredential(credentialStore, "keystore", "password");
      addCredential(credentialStore, "oauth2", "1fdca4ec-c416-47e0-867a-3d471af7050f");
      addCredential(credentialStore, "trust", "secret");
   }
}
