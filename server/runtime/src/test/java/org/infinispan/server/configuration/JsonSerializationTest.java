package org.infinispan.server.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.util.Iterator;
import java.util.Properties;

import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.server.Server;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @since 10.0
 */
public class JsonSerializationTest {

   private FileLookup fileLookup = FileLookupFactory.newInstance();
   private ObjectMapper objectMapper = new ObjectMapper();
   private Properties properties = new Properties();

   private ServerConfiguration parse() throws Exception {
      properties.setProperty(Server.INFINISPAN_SERVER_CONFIG_PATH, System.getProperty("build.directory") + "/test-classes/configuration");
      properties.setProperty("test-path", "/path");
      URL url = fileLookup.lookupFileLocation("configuration/ServerConfigurationParserTest.xml", JsonSerializationTest.class.getClassLoader());
      ParserRegistry registry = new ParserRegistry(this.getClass().getClassLoader(), false, properties);
      ConfigurationBuilderHolder holder = registry.parse(url);
      GlobalConfiguration global = holder.getGlobalConfigurationBuilder().build();
      return global.module(ServerConfiguration.class);
   }

   @Test
   public void testJsonSerialization() throws Exception {
      ServerConfiguration serverConfiguration = parse();

      JsonWriter writer = new JsonWriter();
      String json = writer.toJSON(serverConfiguration);
      JsonNode serverNode = objectMapper.readTree(json).get("server");

      JsonNode interfaces = serverNode.get("interfaces").get("interface");
      assertEquals(2, interfaces.size());

      JsonNode interface1 = interfaces.get(0);
      JsonNode interface2 = interfaces.get(1);
      JsonNode address1 = interface1.get("loopback");
      JsonNode address2 = interface2.get("loopback");
      assertEquals("default", interface1.get("name").asText());
      assertEquals(0, address1.size());
      assertEquals("another", interface2.get("name").asText());
      assertEquals(0, address2.size());

      JsonNode socketBindings = serverNode.get("socket-bindings");
      assertEquals("default", socketBindings.get("default-interface").asText());
      assertEquals(0, socketBindings.get("port-offset").asInt());

      JsonNode socketBinding = socketBindings.get("socket-binding");
      assertEquals(2, socketBinding.size());

      Iterator<JsonNode> bindings = socketBinding.elements();
      JsonNode binding1 = bindings.next();
      assertEquals("default", binding1.get("name").asText());
      assertEquals(11222, binding1.get("port").asInt());
      JsonNode binding2 = bindings.next();
      assertEquals("memcached", binding2.get("name").asText());
      assertEquals(11221, binding2.get("port").asInt());

      JsonNode securityRealms = serverNode.get("security").get("security-realms");
      assertEquals(1, securityRealms.size());

      JsonNode securityRealm = securityRealms.get("security-realm");
      assertEquals("default", securityRealm.get("name").asText());

      JsonNode ssl = securityRealm.get("server-identities").get("ssl");
      JsonNode keyStore = ssl.get("keystore");
      assertEquals("ServerConfigurationParserTest-keystore.pfx", keyStore.get("path").asText());
      assertEquals(properties.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH), keyStore.get("relative-to").asText());
      assertEquals("***", keyStore.get("keystore-password").asText());
      assertEquals("server", keyStore.get("alias").asText());
      assertEquals("***", keyStore.get("key-password").asText());
      assertEquals("localhost", keyStore.get("generate-self-signed-certificate-host").asText());
      JsonNode engine = ssl.get("engine");
      JsonNode protocols = engine.get("enabled-protocols");
      Iterator<JsonNode> protocolItems = protocols.elements();
      assertEquals("TLSV1.1", protocolItems.next().asText());
      assertEquals("TLSV1.2", protocolItems.next().asText());
      assertEquals("TLSV1.3", protocolItems.next().asText());
      JsonNode cipherSuites = engine.get("enabled-ciphersuites");
      assertEquals("DHE-RSA-AES256-GCM-SHA384:DHE-RSA-AES128-GCM-SHA256", cipherSuites.asText());

      JsonNode filesystemRealm = securityRealm.get("filesystem-realm");
      assertEquals("security", filesystemRealm.get("path").asText());
      assertEquals(3, filesystemRealm.get("levels").asInt());
      assertFalse(filesystemRealm.get("encoded").asBoolean());
      assertEquals(properties.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH), filesystemRealm.get("relative-to").asText());

      JsonNode ldapRealm = securityRealm.get("ldap-realm");
      assertEquals("ldap", ldapRealm.get("name").asText());
      assertEquals("ldap://${org.infinispan.test.host.address}:10389", ldapRealm.get("url").asText());
      assertEquals("uid=admin,ou=People,dc=infinispan,dc=org", ldapRealm.get("principal").asText());
      assertEquals("***", ldapRealm.get("credential").asText());
      JsonNode ldapIdentityMapping = ldapRealm.get("identity-mapping");
      assertEquals("uid", ldapIdentityMapping.get("rdn-identifier").asText());
      assertEquals("ou=People,dc=infinispan,dc=org", ldapIdentityMapping.get("search-base-dn").asText());
      JsonNode attributeMapping = ldapIdentityMapping.get("attribute-mapping");
      JsonNode attributes = attributeMapping.get("attribute");
      assertEquals(2, attributes.size());
      Iterator<JsonNode> elements = attributes.elements();
      JsonNode attribute1 = elements.next();
      assertEquals("cn", attribute1.get("from").asText());
      assertEquals("Roles", attribute1.get("to").asText());
      assertEquals("(&(objectClass=groupOfNames)(member={1}))", attribute1.get("filter").asText());
      assertEquals("ou=Roles,dc=infinispan,dc=org", attribute1.get("filter-dn").asText());
      JsonNode attribute2 = elements.next();
      assertEquals("cn2", attribute2.get("from").asText());
      assertEquals("Roles2", attribute2.get("to").asText());
      assertEquals("(&(objectClass=GroupOfUniqueNames)(member={0}))", attribute2.get("filter").asText());
      assertEquals("ou=People,dc=infinispan,dc=org", attribute2.get("filter-dn").asText());
      JsonNode userPasswordMapping = ldapIdentityMapping.get("user-password-mapper");
      assertEquals("userPassword", userPasswordMapping.get("from").asText());
      assertFalse(userPasswordMapping.get("verifiable").asBoolean());
      assertFalse(userPasswordMapping.get("writable").asBoolean());

      JsonNode localRealm = securityRealm.get("local-realm");
      assertEquals("test-local", localRealm.get("name").asText());

      JsonNode kerberosRealm = securityRealm.get("kerberos-realm");
      assertEquals("keytab", kerberosRealm.get("keytab-path").asText());
      assertEquals(properties.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH), kerberosRealm.get("relative-to").asText());

      JsonNode propertiesRealm = securityRealm.get("properties-realm");
      assertEquals("Roles", propertiesRealm.get("groups-attribute").asText());
      JsonNode userProperties = propertiesRealm.get("user-properties");
      assertEquals("ServerConfigurationParserTest-user.properties", userProperties.get("path").asText());
      assertEquals("digest", userProperties.get("digest-realm-name").asText());
      assertEquals(properties.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH), userProperties.get("relative-to").asText());
      assertTrue(userProperties.get("plain-text").asBoolean());
      JsonNode groupProperties = propertiesRealm.get("group-properties");
      assertEquals(properties.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH), groupProperties.get("relative-to").asText());
      assertEquals("ServerConfigurationParserTest-group.properties", groupProperties.get("path").asText());

      JsonNode tokenRealm = securityRealm.get("token-realm");
      assertEquals("token-test", tokenRealm.get("name").asText());
      assertEquals("username-claim", tokenRealm.get("principal-claim").asText());
      JsonNode oath = tokenRealm.get("oauth2-introspection");
      assertEquals("ANY", oath.get("host-name-verification-policy").asText());
      assertEquals("http://${org.infinispan.test.host .address}:14567/auth/realms/infinispan/protocol/openid-connect/token/introspect", oath.get("introspection-url").asText());
      assertEquals("infinispan-server", oath.get("client-id").asText());
      assertEquals("***", oath.get("client-secret").asText());

      JsonNode trustStoreRealm = securityRealm.get("truststore-realm");
      assertEquals("truststore.p12", trustStoreRealm.get("path").asText());
      assertEquals("SunJSSE", trustStoreRealm.get("provider").asText());
      assertEquals(properties.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH), trustStoreRealm.get("relative-to").asText());
      assertEquals("***", trustStoreRealm.get("keystore-password").asText());

   }
}
