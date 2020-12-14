package org.infinispan.server.configuration;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.dataconversion.internal.Json;
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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
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

public class ServerConfigurationParserTest {

   @ClassRule
   public static final JUnitThreadTrackerRule tracker = new JUnitThreadTrackerRule();

   @BeforeClass
   public static void setUp() {
      registerSecurityProviders();
      createCredentialStore(getConfigPath().resolve("credentials.pfx"), "secret");
   }

   @Test
   public void testXMLParser() throws IOException {
      validateConfiguration(parse(".xml"));
   }

   @Test
   public void testYAMLParser() throws IOException {
      validateConfiguration(parse(".yml"));
   }

   private void validateConfiguration(ServerConfiguration server) {
      // Interfaces
      assertEquals(2, server.networkInterfaces().size());
      NetworkAddress defaultInterface = server.networkInterfaces().get("default");
      assertNotNull(defaultInterface);
      assertTrue(defaultInterface.getAddress().isLoopbackAddress());

      // Socket bindings
      Map<String, SocketBinding> socketBindings = server.socketBindings();
      assertEquals(5, socketBindings.size());
      assertEquals(11221, socketBindings.get("memcached").getPort());
      assertEquals(12221, socketBindings.get("memcached-2").getPort());
      assertEquals(11222, socketBindings.get("default").getPort());
      assertEquals(11223, socketBindings.get("hotrod").getPort());
      assertEquals(8080, socketBindings.get("rest").getPort());

      // Security realms
      List<RealmConfiguration> realms = server.security().realms().realms();
      assertEquals(2, realms.size());
      RealmConfiguration realmConfiguration = realms.get(0);
      assertEquals("default", realmConfiguration.name());

      realmConfiguration = realms.get(1);
      assertEquals("using-credentials", realmConfiguration.name());

      // Data Sources
      Map<String, DataSourceConfiguration> dataSources = server.dataSources();
      assertEquals(2, dataSources.size());
      DataSourceConfiguration dataSource = dataSources.get("database");
      assertEquals("jdbc/database", dataSource.jndiName());
      assertEquals("jdbc:h2:tcp://${org.infinispan.test.host.address}:1521/test", dataSource.url());
      assertEquals("test", dataSource.username());
      assertEquals("test", dataSource.password());
      assertEquals("SELECT 1", dataSource.initialSql());
      assertEquals("org.h2.Driver", dataSource.driver());
      assertEquals(10, dataSource.maxSize());
      assertEquals(1, dataSource.minSize());
      assertEquals(1, dataSource.initialSize());
      assertEquals(1, dataSource.connectionProperties().size());
      assertEquals("somevalue", dataSource.connectionProperties().get("someproperty"));
      dataSource = dataSources.get("database-with-credential");
      assertEquals("test", dataSource.password());

      // Connectors
      List<ProtocolServerConfiguration> connectors = server.endpoints().endpoints().get(0).connectors();
      assertEquals(3, connectors.size());
      assertTrue(connectors.get(0) instanceof HotRodServerConfiguration);
      assertTrue(connectors.get(1) instanceof RestServerConfiguration);
      assertTrue(connectors.get(2) instanceof MemcachedServerConfiguration);

      // Ensure endpoints are bound to the interfaces
      SinglePortRouterConfiguration singlePortRouter = server.endpoints().endpoints().get(0).singlePortRouter();
      assertEquals(socketBindings.get("default").getAddress().getAddress().getHostAddress(), singlePortRouter.host());
      assertEquals(socketBindings.get("default").getPort(), singlePortRouter.port());
      assertEquals(socketBindings.get("memcached").getPort(), server.endpoints().endpoints().get(0).connectors().get(2).port());

      assertEquals("strongPassword", realmConfiguration.ldapConfiguration().attributes().attribute("credential").get());
      assertEquals("secret", new String((char[]) realmConfiguration.serverIdentitiesConfiguration().sslConfiguration().trustStore().attributes().attribute("password").get())); //stores it as char[]
      assertEquals("1fdca4ec-c416-47e0-867a-3d471af7050f", realmConfiguration.tokenConfiguration().oauth2Configuration().attributes().attribute("clientSecret").get());
      assertEquals("password", new String((char[]) realmConfiguration.serverIdentitiesConfiguration().sslConfiguration().keyStore().attributes().attribute("keystorePassword").get()));
   }

   @Test
   public void testJsonSerialization() throws IOException {
      ServerConfiguration serverConfiguration = parse(".xml");

      String serverConfigPath = getConfigPath().toString();

      JsonWriter writer = new JsonWriter();
      String json = writer.toJSON(serverConfiguration);

      Json serverNode = Json.read(json).at("server");

      Json interfaces = serverNode.at("interfaces").at("interface");
      assertEquals(2, interfaces.asList().size());

      Json interface1 = interfaces.at(0);
      Json interface2 = interfaces.at(1);
      Json address1 = interface1.at("loopback");
      Json address2 = interface2.at("loopback");
      assertEquals("default", interface1.at("name").asString());
      assertEquals(0, address1.asMap().size());
      assertEquals("another", interface2.at("name").asString());
      assertEquals(0, address2.asMap().size());

      Json socketBindings = serverNode.at("socket-bindings");
      assertEquals("default", socketBindings.at("default-interface").asString());
      assertEquals(0, socketBindings.at("port-offset").asInteger());

      Json socketBinding = socketBindings.at("socket-binding");
      assertEquals(5, socketBinding.asList().size());

      Iterator<Json> bindings = socketBinding.asJsonList().iterator();
      Json binding1 = bindings.next();
      assertEquals("default", binding1.at("name").asString());
      assertEquals(11222, binding1.at("port").asInteger());
      Json binding2 = bindings.next();
      assertEquals("hotrod", binding2.at("name").asString());
      assertEquals(11223, binding2.at("port").asInteger());
      Json binding3 = bindings.next();
      assertEquals("memcached", binding3.at("name").asString());
      assertEquals(11221, binding3.at("port").asInteger());
      Json binding4 = bindings.next();
      assertEquals("memcached-2", binding4.at("name").asString());
      assertEquals(12221, binding4.at("port").asInteger());
      Json binding5 = bindings.next();
      assertEquals("rest", binding5.at("name").asString());
      assertEquals(8080, binding5.at("port").asInteger());

      Json credentialStores = serverNode.at("security").at("credential-stores");
      assertEquals(1, credentialStores.asMap().size());
      Json credentialStore = credentialStores.at("credential-store");
      assertEquals("credentials.pfx", credentialStore.at("path").asString());

      Json securityRealms = serverNode.at("security").at("security-realms");
      assertEquals(1, securityRealms.asMap().size());

      Json securityRealm = securityRealms.at("security-realm");
      assertEquals(2, securityRealm.asList().size());
      Json defaultRealm = securityRealm.at(0);
      assertEquals("default", defaultRealm.at("name").asString());

      Json ssl = defaultRealm.at("server-identities").at("ssl");
      Json keyStore = ssl.at("keystore");
      assertEquals("ServerConfigurationParserTest-keystore.pfx", keyStore.at("path").asString());
      assertEquals(serverConfigPath, keyStore.at("relative-to").asString());
      assertEquals("***", keyStore.at("keystore-password").asString());
      assertEquals("server", keyStore.at("alias").asString());
      assertEquals("***", keyStore.at("key-password").asString());
      assertEquals("localhost", keyStore.at("generate-self-signed-certificate-host").asString());
      Json engine = ssl.at("engine");
      Json protocols = engine.at("enabled-protocols");
      Iterator<Json> protocolItems = protocols.asJsonList().iterator();
      assertEquals("TLSV1.1", protocolItems.next().asString());
      assertEquals("TLSV1.2", protocolItems.next().asString());
      assertEquals("TLSV1.3", protocolItems.next().asString());
      Json cipherSuites = engine.at("enabled-ciphersuites");
      assertEquals("DHE-RSA-AES256-GCM-SHA384:DHE-RSA-AES128-GCM-SHA256", cipherSuites.asString());

      Json kerberos = defaultRealm.at("server-identities").at("kerberos");
      assertEquals("keytab", kerberos.at("keytab-path").asString());
      assertEquals(serverConfigPath, kerberos.at("relative-to").asString());

      Json filesystemRealm = defaultRealm.at("filesystem-realm");
      assertEquals("security", filesystemRealm.at("path").asString());
      assertEquals(3, filesystemRealm.at("levels").asInteger());
      assertFalse(filesystemRealm.at("encoded").asBoolean());
      assertEquals(serverConfigPath, filesystemRealm.at("relative-to").asString());

      Json ldapRealm = defaultRealm.at("ldap-realm");
      assertEquals("ldap", ldapRealm.at("name").asString());
      assertEquals("ldap://${org.infinispan.test.host.address}:10389", ldapRealm.at("url").asString());
      assertEquals("uid=admin,ou=People,dc=infinispan,dc=org", ldapRealm.at("principal").asString());
      assertEquals("***", ldapRealm.at("credential").asString());

      assertEquals("org.wildfly.security.auth.util.RegexNameRewriter", ldapRealm.at("name-rewriter").asString());

      Json ldapIdentityMapping = ldapRealm.at("identity-mapping");
      assertEquals("uid", ldapIdentityMapping.at("rdn-identifier").asString());
      assertEquals("ou=People,dc=infinispan,dc=org", ldapIdentityMapping.at("search-base-dn").asString());
      Json attributeMapping = ldapIdentityMapping.at("attribute-mapping");
      Json attributes = attributeMapping.at("attribute");
      assertEquals(3, attributes.asList().size());
      Iterator<Json> elements = attributes.asJsonList().iterator();
      Json attribute1 = elements.next();
      assertEquals("cn", attribute1.at("from").asString());
      assertEquals("Roles", attribute1.at("to").asString());
      assertEquals("(&(objectClass=groupOfNames)(member={1}))", attribute1.at("filter").asString());
      assertEquals("ou=Roles,dc=infinispan,dc=org", attribute1.at("filter-dn").asString());
      Json attribute2 = elements.next();
      assertEquals("cn2", attribute2.at("from").asString());
      assertEquals("Roles2", attribute2.at("to").asString());
      assertEquals("(&(objectClass=GroupOfUniqueNames)(member={0}))", attribute2.at("filter").asString());
      assertEquals("ou=People,dc=infinispan,dc=org", attribute2.at("filter-dn").asString());
      Json attribute3 = elements.next();
      assertEquals("memberOf", attribute3.at("reference").asString());
      assertEquals("Roles3", attribute3.at("to").asString());
      Json userPasswordMapping = ldapIdentityMapping.at("user-password-mapper");
      assertEquals("userPassword", userPasswordMapping.at("from").asString());
      assertFalse(userPasswordMapping.at("verifiable").asBoolean());
      assertFalse(userPasswordMapping.at("writable").asBoolean());

      Json localRealm = defaultRealm.at("local-realm");
      assertEquals("test-local", localRealm.at("name").asString());

      Json propertiesRealm = defaultRealm.at("properties-realm");
      assertEquals("Roles", propertiesRealm.at("groups-attribute").asString());
      Json userProperties = propertiesRealm.at("user-properties");
      assertEquals("ServerConfigurationParserTest-user.properties", userProperties.at("path").asString());
      assertEquals("digest", userProperties.at("digest-realm-name").asString());
      assertEquals(serverConfigPath, userProperties.at("relative-to").asString());
      assertTrue(userProperties.at("plain-text").asBoolean());
      Json groupProperties = propertiesRealm.at("group-properties");
      assertEquals(serverConfigPath, groupProperties.at("relative-to").asString());
      assertEquals("ServerConfigurationParserTest-group.properties", groupProperties.at("path").asString());

      Json tokenRealm = defaultRealm.at("token-realm");
      assertEquals("token-test", tokenRealm.at("name").asString());
      assertEquals("username-claim", tokenRealm.at("principal-claim").asString());
      Json oauth = tokenRealm.at("oauth2-introspection");
      assertEquals("ANY", oauth.at("host-name-verification-policy").asString());
      assertEquals("http://${org.infinispan.test.host.address}:14567/auth/realms/infinispan/protocol/openid-connect/token/introspect", oauth.at("introspection-url").asString());
      assertEquals("infinispan-server", oauth.at("client-id").asString());
      assertEquals("***", oauth.at("client-secret").asString());

      Json trustStoreRealm = defaultRealm.at("truststore-realm");
      assertEquals(1, trustStoreRealm.asMap().size());
      assertEquals("trust", trustStoreRealm.at("name").asString());

      Json endpoints = serverNode.at("endpoints");
      assertEquals("default", endpoints.at("socket-binding").asString());
      assertEquals("default", endpoints.at("security-realm").asString());

      Json hotrodConnector = endpoints.at("hotrod-connector");
      Json restConnector = endpoints.at("rest-connector");
      Json memcachedConnector = endpoints.at("memcached-connector");
      assertHotRodConnector(hotrodConnector);
      assertRestConnector(restConnector);
      assertMemcachedConnector(memcachedConnector);
   }

   ServerConfiguration parse(String extension) throws IOException {
      FileLookup fileLookup = FileLookupFactory.newInstance();
      URL url = fileLookup.lookupFileLocation("configuration/" + getClass().getSimpleName() + extension, ServerConfigurationParserTest.class.getClassLoader());
      Properties properties = new Properties();
      properties.setProperty(Server.INFINISPAN_SERVER_CONFIG_PATH, getConfigPath().toString());
      ParserRegistry registry = new ParserRegistry(this.getClass().getClassLoader(), false, properties);
      ConfigurationBuilderHolder holder = registry.parse(url);
      GlobalConfiguration global = holder.getGlobalConfigurationBuilder().build();
      return global.module(ServerConfiguration.class);
   }

   private void assertHotRodConnector(Json hotrodConnector) {
      assertEquals("hotrod", hotrodConnector.at("name").asString());
      assertEquals(23, hotrodConnector.at("io-threads").asInteger());
      assertFalse(hotrodConnector.at("tcp-nodelay").asBoolean());
      assertEquals(20, hotrodConnector.at("worker-threads").asInteger());
      assertFalse(hotrodConnector.at("tcp-keepalive").asBoolean());
      assertEquals(10, hotrodConnector.at("send-buffer-size").asInteger());
      assertEquals(20, hotrodConnector.at("receive-buffer-size").asInteger());
      assertEquals(2, hotrodConnector.at("idle-timeout").asInteger());
      assertEquals("hotrod", hotrodConnector.at("socket-binding").asString());
      assertEquals("external", hotrodConnector.at("external-host").asString());
      assertEquals(12345, hotrodConnector.at("external-port").asInteger());

      Json topologyCache = hotrodConnector.at("topology-state-transfer");
      assertFalse(topologyCache.at("await-initial-retrieval").asBoolean());
      assertFalse(topologyCache.at("lazy-retrieval").asBoolean());
      assertEquals(12, topologyCache.at("lock-timeout").asInteger());
      assertEquals(13, topologyCache.at("replication-timeout").asInteger());

      Json authentication = hotrodConnector.at("authentication");
      assertEquals("default", authentication.at("security-realm").asString());

      Json sasl = authentication.at("sasl");
      assertEquals("localhost", sasl.at("server-name").asString());

      Iterator<Json> mechanisms = sasl.at("mechanisms").asJsonList().iterator();
      assertEquals("GSSAPI", mechanisms.next().asString());
      assertEquals("DIGEST-MD5", mechanisms.next().asString());
      assertEquals("PLAIN", mechanisms.next().asString());

      Iterator<Json> qop = sasl.at("qop").asJsonList().iterator();
      assertEquals("auth", qop.next().asString());
      assertEquals("auth-conf", qop.next().asString());

      Iterator<Json> strength = sasl.at("strength").asJsonList().iterator();
      assertEquals("high", strength.next().asString());
      assertEquals("medium", strength.next().asString());
      assertEquals("low", strength.next().asString());

      Json policy = sasl.at("policy");
      assertFalse(policy.at("forward-secrecy").at("value").asBoolean());
      assertTrue(policy.at("no-active").at("value").asBoolean());
      assertTrue(policy.at("no-anonymous").at("value").asBoolean());
      assertFalse(policy.at("no-dictionary").at("value").asBoolean());
      assertTrue(policy.at("no-plain-text").at("value").asBoolean());
      assertTrue(policy.at("pass-credentials").at("value").asBoolean());

      Json extraProperties = sasl.at("property");
      assertEquals("value1", extraProperties.at("prop1").asString());
      assertEquals("value2", extraProperties.at("prop2").asString());
      assertEquals("value3", extraProperties.at("prop3").asString());

      Json encryption = hotrodConnector.at("encryption");
      assertTrue(encryption.at("require-ssl-client-auth").asBoolean());
      assertEquals("default", encryption.at("security-realm").asString());

      Json sni = encryption.at("sni");
      assertEquals(2, sni.asList().size());
      Iterator<Json> elements = sni.asJsonList().iterator();
      Json sni1 = elements.next();
      assertEquals("sni-host-1", sni1.at("host-name").asString());
      assertEquals("default", sni1.at("security-realm").asString());
      Json sni2 = elements.next();
      assertEquals("sni-host-2", sni2.at("host-name").asString());
      assertEquals("default", sni2.at("security-realm").asString());
   }

   private void assertRestConnector(Json restConnector) {
      assertEquals("rest", restConnector.at("socket-binding").asString());
      assertEquals(11, restConnector.at("io-threads").asInteger());
      assertEquals(3, restConnector.at("worker-threads").asInteger());
      assertEquals("rest", restConnector.at("name").asString());
      assertEquals("rest", restConnector.at("context-path").asString());
      assertEquals("NEVER", restConnector.at("extended-headers").asString());
      assertEquals(3, restConnector.at("max-content-length").asInteger());
      assertEquals(3, restConnector.at("compression-level").asInteger());

      Json authentication = restConnector.at("authentication");
      assertEquals("default", authentication.at("security-realm").asString());
      Json mechanisms = authentication.at("mechanisms");
      assertEquals(2, mechanisms.asList().size());

      Iterator<Json> items = mechanisms.asJsonList().iterator();
      assertEquals("DIGEST", items.next().asString());
      assertEquals("BASIC", items.next().asString());

      Json corsRules = restConnector.at("cors-rules").at("cors-rule");
      assertEquals(2, corsRules.asList().size());
      Iterator<Json> rules = corsRules.asJsonList().iterator();
      Json rule1 = rules.next();
      assertEquals("rule1", rule1.at("name").asString());
      assertTrue(rule1.at("allow-credentials").asBoolean());
      assertEquals(1, rule1.at("max-age-seconds").asInteger());
      assertStringArray(asList("origin1", "origin2"), rule1.at("allowed-origins"));
      assertStringArray(asList("GET", "POST"), rule1.at("allowed-methods"));
      assertStringArray(singletonList("Accept"), rule1.at("allowed-headers"));
      assertStringArray(asList("Accept", "Content-Type"), rule1.at("expose-headers"));

      Json rule2 = rules.next();
      assertEquals("rule2", rule2.at("name").asString());
      assertStringArray(singletonList("*"), rule2.at("allowed-origins"));
      assertStringArray(asList("GET", "POST"), rule2.at("allowed-methods"));
      assertNull(rule2.at("allowed-headers"));
      assertNull(rule2.at("expose-headers"));

      Json encryption = restConnector.at("encryption");
      assertFalse(encryption.at("require-ssl-client-auth").asBoolean());
      assertEquals("default", encryption.at("security-realm").asString());

      Json sni = encryption.at("sni");
      assertEquals(2, sni.asList().size());
      Iterator<Json> elements = sni.asJsonList().iterator();
      Json sni1 = elements.next();
      assertEquals("sni-host-3", sni1.at("host-name").asString());
      assertEquals("default", sni1.at("security-realm").asString());
      Json sni2 = elements.next();
      assertEquals("sni-host-4", sni2.at("host-name").asString());
      assertEquals("default", sni2.at("security-realm").asString());
   }

   private void assertMemcachedConnector(Json memcachedConnector) {
      assertEquals("memcached", memcachedConnector.at("name").asString());
      assertEquals("memcached", memcachedConnector.at("socket-binding").asString());
      assertEquals(1, memcachedConnector.at("io-threads").asInteger());
      assertEquals(160, memcachedConnector.at("worker-threads").asInteger());
      assertEquals(1, memcachedConnector.at("idle-timeout").asInteger());
      assertTrue(memcachedConnector.at("tcp-nodelay").asBoolean());
      assertTrue(memcachedConnector.at("tcp-keepalive").asBoolean());
      assertEquals(3, memcachedConnector.at("send-buffer-size").asInteger());
      assertEquals(3, memcachedConnector.at("receive-buffer-size").asInteger());
      assertEquals("string", memcachedConnector.at("cache").asString());
      assertEquals("application/json", memcachedConnector.at("client-encoding").asString());
   }

   private void assertStringArray(List<String> expected, Json actual) {
      List<String> elements = actual.asJsonList().stream().map(Json::asString).collect(Collectors.toList());
      assertEquals(expected, elements);
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
