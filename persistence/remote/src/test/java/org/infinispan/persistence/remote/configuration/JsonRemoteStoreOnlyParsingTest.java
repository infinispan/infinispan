package org.infinispan.persistence.remote.configuration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.List;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.remote.upgrade.SerializationUtils;
import org.testng.annotations.Test;

/**
 * Test parsing of a JSON document containing only a remote-store config
 *
 * @since 13.0
 */
@Test(groups = "unit", testName = "persistence.remote.configuration.JsonRemoteStoreOnlyParsingTest")
public class JsonRemoteStoreOnlyParsingTest {

   @Test
   public void testJsonParsing() throws IOException {
      String json = "{\n" +
            "    \"remote-store\":{\n" +
            "        \"cache\":\"ccc\",\n" +
            "        \"shared\":true,\n" +
            "        \"read-only\":false,\n" +
            "        \"hotrod-wrapping\":false,\n" +
            "        \"raw-values\":false,\n" +
            "        \"socket-timeout\":60000,\n" +
            "        \"protocol-version\":\"2.8\",\n" +
            "        \"remote-server\":[\n" +
            "            {\n" +
            "                \"host\":\"127.0.0.2\",\n" +
            "                \"port\":12222\n" +
            "            }\n" +
            "        ],\n" +
            "        \"connection-pool\":{\n" +
            "            \"max-active\":110,\n" +
            "            \"exhausted-action\":\"CREATE_NEW\"\n" +
            "        },\n" +
            "        \"async-executor\":{\n" +
            "            \"properties\":{\n" +
            "                \"name\":4\n" +
            "            }\n" +
            "        },\n" +
            "        \"properties\":{\n" +
            "                \"key\":\"value\"\n" +
            "        },\n" +
            "        \"security\":{\n" +
            "            \"authentication\":{\n" +
            "                \"server-name\":\"servername\",\n" +
            "                \"digest\":{\n" +
            "                    \"username\":\"username\",\n" +
            "                    \"password\":\"password\",\n" +
            "                    \"realm\":\"realm\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"encryption\":{\n" +
            "                \"protocol\":\"TLSv1.2\",\n" +
            "                \"sni-hostname\":\"snihostname\",\n" +
            "                \"keystore\":{\n" +
            "                    \"filename\":\"${project.build.testOutputDirectory}/keystore_client.jks\",\n" +
            "                    \"password\":\"secret\",\n" +
            "                    \"certificate-password\":\"secret\",\n" +
            "                    \"key-alias\":\"hotrod\",\n" +
            "                    \"type\":\"JKS\"\n" +
            "                },\n" +
            "                \"truststore\":{\n" +
            "                    \"filename\":\"${project.build.testOutputDirectory}/ca.jks\",\n" +
            "                    \"type\":\"pem\"\n" +
            "                }\n" +
            "            }\n" +
            "        }\n" +
            "    }\n" +
            "}";


      RemoteStoreConfiguration store = SerializationUtils.fromJson(json);

      assertEquals("ccc", store.remoteCacheName());
      assertTrue(store.shared());
      assertFalse(store.ignoreModifications());
      assertFalse(store.hotRodWrapping());
      assertFalse(store.rawValues());
      assertEquals(60000, store.socketTimeout());
      assertEquals(ProtocolVersion.PROTOCOL_VERSION_28, store.protocol());

      List<RemoteServerConfiguration> servers = store.servers();
      RemoteServerConfiguration firstServer = servers.iterator().next();
      assertEquals(1, servers.size());
      assertEquals("127.0.0.2", firstServer.host());
      assertEquals(12222, firstServer.port());

      TypedProperties asyncExecutorProps = store.asyncExecutorFactory().properties();
      assertEquals(1, asyncExecutorProps.size());
      assertEquals(4, asyncExecutorProps.getLongProperty("name", 0L));

      ConnectionPoolConfiguration poolConfiguration = store.connectionPool();
      assertEquals(ExhaustedAction.CREATE_NEW, poolConfiguration.exhaustedAction());
      assertEquals(110, poolConfiguration.maxActive());

      AuthenticationConfiguration authentication = store.security().authentication();
      assertEquals("servername", authentication.serverName());
      MechanismConfiguration mechanismConfiguration = authentication.mechanismConfiguration();
      assertEquals(mechanismConfiguration.saslMechanism(), "DIGEST-MD5");

      SslConfiguration ssl = store.security().ssl();
      assertEquals("snihostname", ssl.sniHostName());
      assertEquals("secret", new String(ssl.keyStorePassword()));
   }

   @Test
   public void testJsonSerializing() {
      ConfigurationBuilder builder = new ConfigurationBuilder();

      builder.persistence().addStore(RemoteStoreConfigurationBuilder.class)
            .remoteCacheName("remote").addServer().host("127.0.0.2").port(1111)
            .remoteSecurity()
            .authentication().enable().saslMechanism("DIGEST-MD5")
            .username("user")
            .password("pass")
            .realm("default");

      RemoteStoreConfiguration remoteStoreConfiguration = (RemoteStoreConfiguration) builder.build().persistence().stores().iterator().next();

      Json serialized = Json.read(SerializationUtils.toJson(remoteStoreConfiguration));

      assertEquals(1, serialized.asJsonMap().size());
      assertNotNull(serialized.at("remote-store"));
   }
}
