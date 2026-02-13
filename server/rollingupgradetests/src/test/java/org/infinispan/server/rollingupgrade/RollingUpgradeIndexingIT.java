package org.infinispan.server.rollingupgrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.test.core.Common.sync;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteSchemasAdmin;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.configuration.BasicConfiguration;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.test.Eventually;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfiguration;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

public class RollingUpgradeIndexingIT {

   @Test
   public void testRollingUpgradeWithIndexes() throws Throwable {
      String cacheName = "rolling-upgrade";
      int nodeCount = 2;
      String xml = """
            <distributed-cache name="%s" statistics="true">
               <persistence passivation="false">
                  <file-store shared="false" />
               </persistence>
               <indexing startup-mode="REINDEX">
                  <indexed-entities>
                     <indexed-entity>sample_bank_account.User</indexed-entity>
                  </indexed-entities>
               </indexing>
            </distributed-cache>
            """.formatted(cacheName);

      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(
            this.getClass().getName(),
            RollingUpgradeTestUtil.getFromVersion(),
            RollingUpgradeTestUtil.getToVersion()
      )
            .configurationUpdater(b -> {
               ProtoStreamMarshaller protoStreamMarshaller = new ProtoStreamMarshaller();
               FileDescriptorSource descriptor = FileDescriptorSource.fromString(TestDomainSCI.INSTANCE.getName(), TestDomainSCI.INSTANCE.getContent());
               protoStreamMarshaller.getSerializationContext().registerProtoFiles(descriptor);
               b.marshaller(protoStreamMarshaller);
               b.addContextInitializer(TestDomainSCI.INSTANCE);
               return b;
            })
            .jgroupsProtocol("tcp")
            .nodeCount(nodeCount)
            .handlers(
                  uh -> handleInitializer(uh, cacheName, new StringConfiguration(xml)),
                  uh -> assertDataIsCorrect("validate", uh, cacheName)
            );

      RollingUpgradeConfiguration configuration = builder.build();
      RollingUpgradeHandler.performUpgrade(configuration);
   }

   private void handleInitializer(RollingUpgradeHandler uh, String cacheName, BasicConfiguration configuration) {
      RemoteSchemasAdmin schemas = uh.getRemoteCacheManager().administration().schemas();
      schemas.createOrUpdate(TestDomainSCI.INSTANCE);
      assertThat(schemas.retrieveError(TestDomainSCI.INSTANCE.getName())).isEmpty();
      RemoteCache<String, User> cache = uh.getRemoteCacheManager()
            .administration()
            .getOrCreateCache(cacheName, configuration);
      cache.put("1", getUser());
      assertDataIsCorrect("init", uh, cacheName);
   }

   private static @NonNull User getUser() {
      User user = new User();
      user.setId(1);
      user.setName("Tom");
      user.setSurname("Cat");
      user.setAccountIds(Collections.emptySet());
      user.setAddresses(Collections.emptyList());
      user.setGender(User.Gender.MALE);
      return user;
   }

   private boolean assertDataIsCorrect(String phase, RollingUpgradeHandler ruh, String cacheName) {
      RestClientConfigurationBuilder restBuilder = new RestClientConfigurationBuilder();
      restBuilder.security().authentication().enable().username(TestUser.ADMIN.getUser()).password(TestUser.ADMIN.getPassword());
      RestClient rest = ruh.rest(0, restBuilder);
      Eventually.eventually(
            "Reindexing incomplete: " + phase,
            () -> {
               RestResponse stats = sync(rest.cache(cacheName).searchStats());
               System.out.println(stats.body());
               return stats.body().contains("types\":{\"sample_bank_account.User\":{\"count\":1");
            },
            ruh.getFromDriver().getTimeout(), 1, TimeUnit.SECONDS);
      RemoteCache<String, User> cache = ruh.getRemoteCacheManager().getCache(cacheName);
      assertThat(cache.size()).isEqualTo(1);
      assertThat(cache.get("1")).isEqualTo(getUser());
      return true;
   }
}
