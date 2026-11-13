package org.infinispan.configuration.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.cache.HotKeysConfigurationTest")
public class HotKeysConfigurationTest extends AbstractInfinispanTest {

   private static final int MAX_TOP_K = 512;

   public void testLargestTopKAccepted() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.statistics().enable().hotKeys().enabled(true).topK(MAX_TOP_K);

      assertThat(builder.build().statistics().hotKeys().topK()).isEqualTo(MAX_TOP_K);
   }

   public void testTopKAboveLimitRejected() {
      assertTopKRejected(MAX_TOP_K + 1);
      assertTopKRejected(Integer.MAX_VALUE);
   }

   public void testNonPositiveTopKRejected() {
      assertTopKRejected(0);
      assertTopKRejected(-1);
   }

   private static void assertTopKRejected(int topK) {
      // The validator may run when the attribute is set or when the configuration is built.
      assertThatThrownBy(() -> {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         builder.statistics().enable().hotKeys().enabled(true).topK(topK);
         builder.build();
      }).isInstanceOf(CacheConfigurationException.class);
   }
}
