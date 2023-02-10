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
import org.springframework.test.context.TestPropertySource;

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
            "spring.main.banner-mode=off"
      })
@TestPropertySource(locations = "classpath:test-application.properties")
public class ApplicationPropertiesTest {

   @Autowired
   private RemoteCacheManager remoteCacheManager;

   @Test
   public void testDefaultClient() throws Exception {
      //when
      Configuration configuration = remoteCacheManager.getConfiguration();
      String hostObtainedFromPropertiesFile = configuration.servers().get(0).host();
      int portObtainedFromPropertiesFile = configuration.servers().get(0).port();

      configuration.asyncExecutorFactory().factoryClass();

      // properties
      assertThat(hostObtainedFromPropertiesFile).isEqualTo("180.567.112.333");
      assertThat(portObtainedFromPropertiesFile).isEqualTo(6668);
      assertThat(configuration.tcpNoDelay()).isFalse();
      assertThat(configuration.tcpKeepAlive()).isTrue();
      assertThat(configuration.clientIntelligence()).isEqualTo(ClientIntelligence.TOPOLOGY_AWARE);
      assertThat(configuration.socketTimeout()).isEqualTo(500);
      assertThat(configuration.connectionTimeout()).isEqualTo(200);
      assertThat(configuration.maxRetries()).isEqualTo(30);
      assertThat(configuration.batchSize()).isEqualTo(91);
      assertThat(configuration.version()).isEqualTo(ProtocolVersion.PROTOCOL_VERSION_24);

      // pool
      assertThat(configuration.connectionPool().maxActive()).isEqualTo(90);
      assertThat(configuration.connectionPool().maxWait()).isEqualTo(20000);
      assertThat(configuration.connectionPool().minIdle()).isEqualTo(1000);
      assertThat(configuration.connectionPool().maxPendingRequests()).isEqualTo(845);
      assertThat(configuration.connectionPool().minEvictableIdleTime()).isEqualTo(9000);
      assertThat(configuration.connectionPool().exhaustedAction()).isEqualTo(ExhaustedAction.CREATE_NEW);

      // Thread pool properties
      assertThat(configuration.asyncExecutorFactory().factoryClass()).isEqualTo(DefaultAsyncExecutorFactory.class);
      // TODO: how to assert thread pool size ? default-executor-factory-pool-size

      // Marshalling properties
      assertThat(configuration.marshallerClass()).isEqualTo(JavaSerializationMarshaller.class);
      assertThat(configuration.keySizeEstimate()).isEqualTo(88889);
      assertThat(configuration.valueSizeEstimate()).isEqualTo(11112);
      assertThat(configuration.forceReturnValues()).isTrue();
      assertThat(configuration.serialWhitelist()).contains("APP-KILLER1", "APP-KILLER2");
      // TODO: Consistent Hash Impl ??
      //assertThat(configuration.consistentHashImpl().getClass().toString()).isEqualTo("");

      // Encryption properties
      assertThat(configuration.security().ssl().enabled()).isTrue();
      assertThat(configuration.security().ssl().keyStoreFileName()).isEqualTo("superKeyStoreFile");
      assertThat(configuration.security().ssl().keyStoreType()).isEqualTo("SKL");
      assertThat(configuration.security().ssl().keyStorePassword().length).isEqualTo(17);
      assertThat(configuration.security().ssl().keyAlias()).isEqualTo("superAliasKey");
      assertThat(configuration.security().ssl().keyStoreCertificatePassword()).hasSize(13);
      assertThat(configuration.security().ssl().trustStoreFileName()).isEqualTo("superTrustFileName");
      assertThat(configuration.security().ssl().trustStorePath()).isNull();
      assertThat(configuration.security().ssl().trustStoreType()).isEqualTo("CKO");
      assertThat(configuration.security().ssl().trustStorePassword().length).isEqualTo(18);
      assertThat(configuration.security().ssl().sniHostName()).isEqualTo("elahost");
      assertThat(configuration.security().ssl().protocol()).isEqualTo("TLSv1.4");

      // authentication
      assertThat(configuration.security().authentication().enabled()).isTrue();
      assertThat(configuration.security().authentication().callbackHandler().getClass()).isEqualTo(BasicCallbackHandler.class);
      assertThat(configuration.security().authentication().saslMechanism()).isEqualTo("my-sasl-mechanism");
      assertThat(configuration.security().authentication().serverName()).isEqualTo("my-server-name");
      BasicCallbackHandler basicCallbackHandler = (BasicCallbackHandler) configuration.security().authentication().callbackHandler();
      NameCallback nameCallback = new NameCallback("test", "test");
      PasswordCallback passwordCallback = new PasswordCallback("test", false);
      RealmCallback realmCallback = new RealmCallback("test", "test");
      basicCallbackHandler.handle(new Callback[]{nameCallback, passwordCallback, realmCallback});
      assertThat(nameCallback.getName()).isEqualTo("oihana");
      assertThat(passwordCallback.getPassword()).isEqualTo("oipass".toCharArray());
      assertThat(realmCallback.getText()).isEqualTo("oirealm");
      assertThat(configuration.security().authentication().saslProperties()).hasSize(2);
      assertThat(configuration.security().authentication().saslProperties()).containsKeys("prop1", "prop2");
      assertThat(configuration.security().authentication().saslProperties()).containsValues("value1", "value2");

      // transactions
      assertThat(configuration.transaction().transactionMode()).isEqualTo(TransactionMode.NON_DURABLE_XA);
      assertThat(configuration.transaction().timeout()).isEqualTo(50000);

      // near cache
      assertThat(configuration.nearCache().mode()).isEqualTo(NearCacheMode.INVALIDATED);
      assertThat(configuration.nearCache().maxEntries()).isEqualTo(2000);
      assertThat(configuration.nearCache().cacheNamePattern().pattern()).isEqualTo("appCache*");

      // xsite
      assertThat(configuration.clusters()).hasSize(1);
      ClusterConfiguration site = configuration.clusters().get(0);
      assertThat(site.getCluster()).extracting("host", "port").containsExactly(tuple("hostOi1", 21222), tuple("hostOi2", 21223));

      // statistics
      assertThat(configuration.statistics().enabled()).isTrue();
      assertThat(configuration.statistics().jmxEnabled()).isTrue();
      assertThat(configuration.statistics().jmxName()).isEqualTo("oiJmx");
      assertThat(configuration.statistics().jmxDomain()).isEqualTo("oiJmxDom");
   }

}
