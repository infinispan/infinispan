package org.infinispan.server.configuration;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
import com.fasterxml.jackson.databind.node.ArrayNode;

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
      assertEquals(5, socketBinding.size());

      Iterator<JsonNode> bindings = socketBinding.elements();
      JsonNode binding1 = bindings.next();
      assertEquals("default", binding1.get("name").asText());
      assertEquals(11222, binding1.get("port").asInt());
      JsonNode binding2 = bindings.next();
      assertEquals("hotrod", binding2.get("name").asText());
      assertEquals(11223, binding2.get("port").asInt());
      JsonNode binding3 = bindings.next();
      assertEquals("memcached", binding3.get("name").asText());
      assertEquals(11221, binding3.get("port").asInt());
      JsonNode binding4 = bindings.next();
      assertEquals("memcached-2", binding4.get("name").asText());
      assertEquals(12221, binding4.get("port").asInt());
      JsonNode binding5 = bindings.next();
      assertEquals("rest", binding5.get("name").asText());
      assertEquals(8080, binding5.get("port").asInt());

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

      JsonNode kerberos = securityRealm.get("server-identities").get("kerberos");
      assertEquals("keytab", kerberos.get("keytab-path").asText());
      assertEquals(properties.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH), kerberos.get("relative-to").asText());

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

      /*JsonNode ldapNameRewriter = ldapRealm.get("name-rewriter");
      JsonNode ldapRegexPrincipalTransformer = ldapNameRewriter.get("regex-principal-transformer");
      assertEquals("uid", ldapRegexPrincipalTransformer.get("name").asText());
      assertEquals("uid", ldapRegexPrincipalTransformer.get("pattern").asText());
      assertEquals("uid", ldapRegexPrincipalTransformer.get("replacement").asText());*/

      JsonNode ldapIdentityMapping = ldapRealm.get("identity-mapping");
      assertEquals("uid", ldapIdentityMapping.get("rdn-identifier").asText());
      assertEquals("ou=People,dc=infinispan,dc=org", ldapIdentityMapping.get("search-base-dn").asText());
      JsonNode attributeMapping = ldapIdentityMapping.get("attribute-mapping");
      JsonNode attributes = attributeMapping.get("attribute");
      assertEquals(3, attributes.size());
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
      JsonNode attribute3 = elements.next();
      assertEquals("memberOf", attribute3.get("reference").asText());
      assertEquals("Roles3", attribute3.get("to").asText());
      JsonNode userPasswordMapping = ldapIdentityMapping.get("user-password-mapper");
      assertEquals("userPassword", userPasswordMapping.get("from").asText());
      assertFalse(userPasswordMapping.get("verifiable").asBoolean());
      assertFalse(userPasswordMapping.get("writable").asBoolean());

      JsonNode localRealm = securityRealm.get("local-realm");
      assertEquals("test-local", localRealm.get("name").asText());

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
      assertEquals("http://${org.infinispan.test.host.address}:14567/auth/realms/infinispan/protocol/openid-connect/token/introspect", oath.get("introspection-url").asText());
      assertEquals("infinispan-server", oath.get("client-id").asText());
      assertEquals("***", oath.get("client-secret").asText());

      JsonNode trustStoreRealm = securityRealm.get("truststore-realm");
      assertEquals("truststore.p12", trustStoreRealm.get("path").asText());
      assertEquals("SunJSSE", trustStoreRealm.get("provider").asText());
      assertEquals(properties.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH), trustStoreRealm.get("relative-to").asText());
      assertEquals("***", trustStoreRealm.get("keystore-password").asText());

      JsonNode endpoints = serverNode.get("endpoints");
      assertEquals("default", endpoints.get("socket-binding").asText());
      assertEquals("default", endpoints.get("security-realm").asText());

      JsonNode hotrodConnector = endpoints.get("hotrod-connector");
      JsonNode restConnector = endpoints.get("rest-connector");
      JsonNode memcachedConnector = endpoints.get("memcached-connector");
      assertHotRodConnector(hotrodConnector);
      assertRestConnector(restConnector);
      assertMemcachedConnector(memcachedConnector);
   }

   private void assertHotRodConnector(JsonNode hotrodConnector) {
      assertEquals("hotrod", hotrodConnector.get("name").asText());
      assertEquals(23, hotrodConnector.get("io-threads").asInt());
      assertFalse(hotrodConnector.get("tcp-nodelay").asBoolean());
      assertEquals(20, hotrodConnector.get("worker-threads").asInt());
      assertFalse(hotrodConnector.get("tcp-keepalive").asBoolean());
      assertEquals(10, hotrodConnector.get("send-buffer-size").asInt());
      assertEquals(20, hotrodConnector.get("receive-buffer-size").asInt());
      assertEquals(2, hotrodConnector.get("idle-timeout").asInt());
      assertEquals("hotrod", hotrodConnector.get("socket-binding").asText());
      assertEquals("external", hotrodConnector.get("external-host").asText());
      assertEquals(12345, hotrodConnector.get("external-port").asInt());

      JsonNode topologyCache = hotrodConnector.get("topology-state-transfer");
      assertFalse(topologyCache.get("await-initial-retrieval").asBoolean());
      assertFalse(topologyCache.get("lazy-retrieval").asBoolean());
      assertEquals(12, topologyCache.get("lock-timeout").asInt());
      assertEquals(13, topologyCache.get("replication-timeout").asInt());

      JsonNode authentication = hotrodConnector.get("authentication");
      assertEquals("default", authentication.get("security-realm").asText());

      JsonNode sasl = authentication.get("sasl");
      assertEquals("localhost", sasl.get("server-name").asText());

      Iterator<JsonNode> mechanisms = sasl.get("mechanisms").elements();
      assertEquals("GSSAPI", mechanisms.next().asText());
      assertEquals("DIGEST-MD5", mechanisms.next().asText());
      assertEquals("PLAIN", mechanisms.next().asText());

      Iterator<JsonNode> qop = sasl.get("qop").elements();
      assertEquals("auth", qop.next().asText());
      assertEquals("auth-conf", qop.next().asText());

      Iterator<JsonNode> strength = sasl.get("strength").elements();
      assertEquals("high", strength.next().asText());
      assertEquals("medium", strength.next().asText());
      assertEquals("low", strength.next().asText());

      JsonNode policy = sasl.get("policy");
      assertFalse(policy.get("forward-secrecy").get("value").asBoolean());
      assertTrue(policy.get("no-active").get("value").asBoolean());
      assertTrue(policy.get("no-anonymous").get("value").asBoolean());
      assertFalse(policy.get("no-dictionary").get("value").asBoolean());
      assertTrue(policy.get("no-plain-text").get("value").asBoolean());
      assertTrue(policy.get("pass-credentials").get("value").asBoolean());

      JsonNode extraProperties = sasl.get("property");
      assertEquals("value1", extraProperties.get("prop1").asText());
      assertEquals("value2", extraProperties.get("prop2").asText());
      assertEquals("value3", extraProperties.get("prop3").asText());

      JsonNode encryption = hotrodConnector.get("encryption");
      assertTrue(encryption.get("require-ssl-client-auth").asBoolean());
      assertEquals("default", encryption.get("security-realm").asText());

      JsonNode sni = encryption.get("sni");
      assertEquals(2, sni.size());
      Iterator<JsonNode> elements = sni.elements();
      JsonNode sni1 = elements.next();
      assertEquals("sni-host-1", sni1.get("host-name").asText());
      assertEquals("default", sni1.get("security-realm").asText());
      JsonNode sni2 = elements.next();
      assertEquals("sni-host-2", sni2.get("host-name").asText());
      assertEquals("default", sni2.get("security-realm").asText());
   }

   private void assertRestConnector(JsonNode restConnector) {
      assertEquals("rest", restConnector.get("socket-binding").asText());
      assertEquals(11, restConnector.get("io-threads").asInt());
      assertEquals(3, restConnector.get("worker-threads").asInt());
      assertEquals("rest", restConnector.get("name").asText());
      assertEquals("rest", restConnector.get("context-path").asText());
      assertEquals("NEVER", restConnector.get("extended-headers").asText());
      assertEquals(3, restConnector.get("max-content-length").asInt());
      assertEquals(3, restConnector.get("compression-level").asInt());

      JsonNode authentication = restConnector.get("authentication");
      assertEquals("default", authentication.get("security-realm").asText());
      JsonNode mechanisms = authentication.get("mechanisms");
      assertEquals(2, mechanisms.size());

      Iterator<JsonNode> items = mechanisms.elements();
      assertEquals("DIGEST", items.next().asText());
      assertEquals("BASIC", items.next().asText());

      JsonNode corsRules = restConnector.get("cors-rules").get("cors-rule");
      assertEquals(2, corsRules.size());
      Iterator<JsonNode> rules = corsRules.elements();
      JsonNode rule1 = rules.next();
      assertEquals("rule1", rule1.get("name").asText());
      assertTrue(rule1.get("allow-credentials").asBoolean());
      assertEquals(1, rule1.get("max-age-seconds").asInt());
      assertStringArray(asList("origin1", "origin2"), rule1.get("allowed-origins"));
      assertStringArray(asList("GET", "POST"), rule1.get("allowed-methods"));
      assertStringArray(singletonList("Accept"), rule1.get("allowed-headers"));
      assertStringArray(asList("Accept", "Content-Type"), rule1.get("expose-headers"));

      JsonNode rule2 = rules.next();
      assertEquals("rule2", rule2.get("name").asText());
      assertStringArray(singletonList("*"), rule2.get("allowed-origins"));
      assertStringArray(asList("GET", "POST"), rule2.get("allowed-methods"));
      assertNull(rule2.get("allowed-headers"));
      assertNull(rule2.get("expose-headers"));

      JsonNode encryption = restConnector.get("encryption");
      assertFalse(encryption.get("require-ssl-client-auth").asBoolean());
      assertEquals("default", encryption.get("security-realm").asText());

      JsonNode sni = encryption.get("sni");
      assertEquals(2, sni.size());
      Iterator<JsonNode> elements = sni.elements();
      JsonNode sni1 = elements.next();
      assertEquals("sni-host-3", sni1.get("host-name").asText());
      assertEquals("default", sni1.get("security-realm").asText());
      JsonNode sni2 = elements.next();
      assertEquals("sni-host-4", sni2.get("host-name").asText());
      assertEquals("default", sni2.get("security-realm").asText());
   }

   private void assertMemcachedConnector(JsonNode memcachedConnector) {
      assertEquals("memcached", memcachedConnector.get("name").asText());
      assertEquals("memcached", memcachedConnector.get("socket-binding").asText());
      assertEquals(1, memcachedConnector.get("io-threads").asInt());
      assertEquals(160, memcachedConnector.get("worker-threads").asInt());
      assertEquals(1, memcachedConnector.get("idle-timeout").asInt());
      assertTrue(memcachedConnector.get("tcp-nodelay").asBoolean());
      assertTrue(memcachedConnector.get("tcp-keepalive").asBoolean());
      assertEquals(3, memcachedConnector.get("send-buffer-size").asInt());
      assertEquals(3, memcachedConnector.get("receive-buffer-size").asInt());
      assertEquals("string", memcachedConnector.get("cache").asText());
      assertEquals("application/json", memcachedConnector.get("client-encoding").asText());
   }

   private void assertMemcachedConnector2(JsonNode memcachedConnector) {
      assertEquals("memcached-2", memcachedConnector.get("name").asText());
      assertEquals("memcached-2", memcachedConnector.get("socket-binding").asText());
   }

   private void assertStringArray(List<String> expected, JsonNode actual) {
      ArrayNode arrayNode = (ArrayNode) actual;
      List<String> elements = StreamSupport
            .stream(arrayNode.spliterator(), false).map(JsonNode::asText).collect(Collectors.toList());
      assertEquals(expected, elements);
   }
}
