package org.infinispan.server.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.CustomStoreConfigurationBuilder;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.tags.Persistence;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Persistence
public class CustomStoreOperationsIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/CustomStoreTest.xml")
               .numServers(1)
               .artifacts(artifacts())
               .runMode(ServerRunMode.CONTAINER)
               .build();

   private static JavaArchive[] artifacts() {
      JavaArchive customStoreJar = ShrinkWrap.create(JavaArchive.class, "custom-store.jar");
      customStoreJar.addClass(CustomNonBlockingStore.class);

      return new JavaArchive[] {customStoreJar};
   }

   @Test
   public void testDefineCustomStoreAndUtilize() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
      configurationBuilder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      configurationBuilder.persistence()
            .addStore(CustomStoreConfigurationBuilder.class)
            .segmented(false)
            .customStoreClass(CustomNonBlockingStore.class);
      RemoteCache<String, String> cache = SERVERS.hotrod().withServerConfiguration(configurationBuilder).create();

      assertEquals("Hello World", cache.get("World"));
   }
}
