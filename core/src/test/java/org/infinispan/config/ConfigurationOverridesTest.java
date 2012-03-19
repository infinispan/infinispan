package org.infinispan.config;

import org.infinispan.distribution.ch.TopologyAwareConsistentHash;
import org.infinispan.interceptors.IsMarshallableInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests that configuration elements are overriden when new configurations are
 * derived from existing ones.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "config.ConfigurationOverridesTest")
public class ConfigurationOverridesTest extends AbstractInfinispanTest {

   public void testConfigOverrides() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);
      try {
         Configuration c = new Configuration().fluent()
               .clustering().hash()
                  .consistentHashClass(TopologyAwareConsistentHash.class)
               .versioning().enable()
               .build();
         Configuration c2 = cm.defineConfiguration("a", c);
         assert c2.getConsistentHashClass().equals(
               TopologyAwareConsistentHash.class.getName());
         assert c2.isEnableVersioning();

         c = new Configuration().fluent().dataContainer()
               .dataContainerClass(QueryableDataContainer.class).build();
         c2 = cm.defineConfiguration("b", c);
         assert c2.getDataContainerClass().equals(
               QueryableDataContainer.class.getName());

         IsMarshallableInterceptor intercept = new IsMarshallableInterceptor();
         c = new Configuration().fluent().customInterceptors()
               .add(intercept).last().build();
         c2 = cm.defineConfiguration("c", c);
         List<CustomInterceptorConfig> intercepts = c2.getCustomInterceptors();
         assert intercepts.size() == 1;
         assert intercepts.get(0).getInterceptor().equals(intercept);

      } finally {
         cm.stop();
      }
   }

}
