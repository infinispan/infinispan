package org.infinispan.server.persistence;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.CustomStoreConfigurationBuilder;
import org.infinispan.server.test.core.category.Persistence;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(Persistence.class)
public class CustomStoreOperationsIT {

   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/CustomStoreTest.xml")
               .numServers(1)
               .artifacts(artifacts())
               .build();

   private static JavaArchive[] artifacts() {
      JavaArchive customStoreJar = ShrinkWrap.create(JavaArchive.class, "custom-store.jar");
      customStoreJar.addClass(CustomNonBlockingStore.class);

      return new JavaArchive[] {customStoreJar};
   }

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testDefineCustomStoreAndUtilize() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.persistence()
            .addStore(CustomStoreConfigurationBuilder.class)
            .customStoreClass(CustomNonBlockingStore.class);
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(configurationBuilder).create();

      assertEquals("Hello World", cache.get("World"));
   }
}
