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
      String json = """
            {
                "remote-store":{
                    "cache":"ccc",
                    "shared":true,
                    "read-only":false,
                    "socket-timeout":60000,
                    "protocol-version":"3.1",
                    "remote-server":[
                        {
                            "host":"127.0.0.2",
                            "port":12222
                        }
                    ],
                    "connection-pool":{
                        "max-active":110,
                        "exhausted-action":"CREATE_NEW"
                    },
                    "async-executor":{
                        "properties":{
                            "name":4
                        }
                    },
                    "properties":{
                            "key":"value"
                    },
                    "security":{
                        "authentication":{
                            "server-name":"servername",
                            "digest":{
                                "username":"username",
                                "password":"password",
                                "realm":"realm"
                            }
                        },
                        "encryption":{
                            "protocol":"TLSv1.2",
                            "sni-hostname":"snihostname",
                            "keystore":{
                                "filename":"${project.build.testOutputDirectory}/keystore_client.jks",
                                "password":"secret",
                                "key-alias":"hotrod",
                                "type":"JKS"
                            },
                            "truststore":{
                                "filename":"${project.build.testOutputDirectory}/ca.jks",
                                "type":"pem"
                            }
                        }
                    }
                }
            }""";


      RemoteStoreConfiguration store = SerializationUtils.fromJson(json);

      assertEquals("ccc", store.remoteCacheName());
      assertTrue(store.shared());
      assertFalse(store.ignoreModifications());
      assertEquals(60000, store.socketTimeout());
      assertEquals(ProtocolVersion.PROTOCOL_VERSION_31, store.protocol());

      List<RemoteServerConfiguration> servers = store.servers();
      RemoteServerConfiguration firstServer = servers.iterator().next();
      assertEquals(1, servers.size());
      assertEquals("127.0.0.2", firstServer.host());
      assertEquals(12222, firstServer.port());

      TypedProperties asyncExecutorProps = store.asyncExecutorFactory().properties();
      assertEquals(1, asyncExecutorProps.size());
      assertEquals(4, asyncExecutorProps.getLongProperty("name", 0L));

      AuthenticationConfiguration authentication = store.security().authentication();
      assertEquals("servername", authentication.serverName());
      MechanismConfiguration mechanismConfiguration = authentication.mechanismConfiguration();
      assertEquals("DIGEST-MD5", mechanismConfiguration.saslMechanism());

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
