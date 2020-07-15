package test.org.infinispan.spring.starter.remote;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ClusterConfiguration;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ExhaustedAction;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory;
import org.infinispan.client.hotrod.security.BasicCallbackHandler;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.spring.starter.remote.InfinispanRemoteAutoConfiguration;
import org.infinispan.spring.starter.remote.InfinispanRemoteCacheManagerAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.RealmCallback;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

@SpringBootTest(
      classes = {
            InfinispanRemoteAutoConfiguration.class,
            InfinispanRemoteCacheManagerAutoConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off",
            "infinispan.remote.client-properties=custom-test-hotrod-client.properties"
      }
)
public class CustomPropertiesTest {

   @Autowired
   private RemoteCacheManager remoteCacheManager;

   @Test
   public void testDefaultClient() throws Exception {
      //when
      Configuration configuration = remoteCacheManager.getConfiguration();
      String hostObtainedFromPropertiesFile = configuration.servers().get(0).host();
      int portObtainedFromPropertiesFile = configuration.servers().get(0).port();

      // Connection
      assertThat(hostObtainedFromPropertiesFile).isEqualTo("127.0.0.1");
      assertThat(portObtainedFromPropertiesFile).isEqualTo(6667);
      assertThat(configuration.tcpNoDelay()).isFalse();
      assertThat(configuration.tcpKeepAlive()).isTrue();
      assertThat(configuration.clientIntelligence()).isEqualTo(ClientIntelligence.TOPOLOGY_AWARE);
      assertThat(configuration.socketTimeout()).isEqualTo(3000);
      assertThat(configuration.connectionTimeout()).isEqualTo(5000);
      assertThat(configuration.maxRetries()).isEqualTo(42);
      assertThat(configuration.batchSize()).isEqualTo(90);
      assertThat(configuration.version()).isEqualTo(ProtocolVersion.PROTOCOL_VERSION_28);

      // Connection pool properties
      assertThat(configuration.connectionPool().maxActive()).isEqualTo(91);
      assertThat(configuration.connectionPool().exhaustedAction()).isEqualTo(ExhaustedAction.EXCEPTION);
      assertThat(configuration.connectionPool().maxWait()).isEqualTo(20001);
      assertThat(configuration.connectionPool().minIdle()).isEqualTo(1001);
      assertThat(configuration.connectionPool().minEvictableIdleTime()).isEqualTo(9001);
      assertThat(configuration.connectionPool().maxPendingRequests()).isEqualTo(846);

      // Thread pool properties
      assertThat(configuration.asyncExecutorFactory().factoryClass()).isEqualTo(DefaultAsyncExecutorFactory.class);
      // TODO: how to assert thread pool size ? default_executor_factory.pool_size

      // Marshalling properties
      assertThat(configuration.marshallerClass()).isEqualTo(JavaSerializationMarshaller.class);
      assertThat(configuration.keySizeEstimate()).isEqualTo(123456);
      assertThat(configuration.valueSizeEstimate()).isEqualTo(789012);
      assertThat(configuration.forceReturnValues()).isTrue();
      assertThat(configuration.serialWhitelist()).contains("SERIAL-KILLER");
      // TODO: Consistent Hash Impl ??
      //assertThat(configuration.consistentHashImpl().getClass().toString()).isEqualTo("");

      // Encryption properties
      assertThat(configuration.security().ssl().enabled()).isTrue();
      assertThat(configuration.security().ssl().keyStoreFileName()).isEqualTo("keyStoreFile");
      assertThat(configuration.security().ssl().keyStoreType()).isEqualTo("JKS");
      assertThat(configuration.security().ssl().keyStorePassword()).hasSize(12);
      assertThat(configuration.security().ssl().keyAlias()).isEqualTo("aliasKey");
      assertThat(configuration.security().ssl().keyStoreCertificatePassword()).hasSize(9);
      assertThat(configuration.security().ssl().trustStoreFileName()).isEqualTo("trustFileName");
      assertThat(configuration.security().ssl().trustStorePath()).isNull();
      assertThat(configuration.security().ssl().trustStoreType()).isEqualTo("LOL");
      assertThat(configuration.security().ssl().trustStorePassword().length).isEqualTo(13);
      assertThat(configuration.security().ssl().sniHostName()).isEqualTo("oihost");
      assertThat(configuration.security().ssl().protocol()).isEqualTo("TLSv1.3");

      // authentication
      assertThat(configuration.security().authentication().enabled()).isTrue();
      assertThat(configuration.security().authentication().saslMechanism()).isEqualTo("DIGEST-MD5");
      assertThat(configuration.security().authentication().callbackHandler()).isInstanceOf(BasicCallbackHandler.class);
      assertThat(configuration.security().authentication().serverName()).isEqualTo("my_ela_server_name");
      BasicCallbackHandler basicCallbackHandler = (BasicCallbackHandler) configuration.security().authentication().callbackHandler();
      NameCallback nameCallback = new NameCallback("test", "test");
      PasswordCallback passwordCallback = new PasswordCallback("test", false);
      RealmCallback realmCallback = new RealmCallback("test", "test");
      basicCallbackHandler.handle(new Callback[]{nameCallback, passwordCallback, realmCallback});
      assertThat(nameCallback.getName()).isEqualTo("elaia");
      assertThat(passwordCallback.getPassword()).isEqualTo("elapass".toCharArray());
      assertThat(realmCallback.getText()).isEqualTo("elarealm");
      assertThat(configuration.security().authentication().saslProperties()).hasSize(1);
      assertThat(configuration.security().authentication().saslProperties()).containsOnlyKeys("prop1");
      assertThat(configuration.security().authentication().saslProperties()).containsValues("value1");

      // Transaction properties
      // TODO: transaction_manager_lookup??
      assertThat(configuration.transaction().transactionMode()).isEqualTo(TransactionMode.FULL_XA);
      assertThat(configuration.transaction().timeout()).isEqualTo(50001);

      // near cache
      assertThat(configuration.nearCache().mode()).isEqualTo(NearCacheMode.INVALIDATED);
      assertThat(configuration.nearCache().maxEntries()).isEqualTo(10000);
      assertThat(configuration.nearCache().cacheNamePattern().pattern()).isEqualTo("nearSuperCache*");

      // xsite
      assertThat(configuration.clusters()).hasSize(2);
      ClusterConfiguration siteA = configuration.clusters().get(0);
      ClusterConfiguration siteB = configuration.clusters().get(1);
      assertThat(siteA.getClusterName()).isEqualTo("siteA");
      assertThat(siteB.getClusterName()).isEqualTo("siteB");
      assertThat(siteA.getCluster()).extracting("host", "port").containsExactly(tuple("hostA1", 11222), tuple("hostA2", 11223));
      assertThat(siteB.getCluster()).extracting("host", "port").containsExactly(tuple("hostB1", 11224), tuple("hostB2", 11225));

      // statistics
      assertThat(configuration.statistics().enabled()).isTrue();
      assertThat(configuration.statistics().jmxEnabled()).isTrue();
      assertThat(configuration.statistics().jmxName()).isEqualTo("elaJmx");
      assertThat(configuration.statistics().jmxDomain()).isEqualTo("elaJmxDom2");
   }
}
