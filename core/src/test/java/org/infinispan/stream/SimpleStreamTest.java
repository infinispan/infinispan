package org.infinispan.stream;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.stream.BaseStreamTest;import org.testng.annotations.Test;import java.lang.Override;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "streams.LocalStreamTest")
public class SimpleStreamTest extends LocalStreamTest {
   @Override
   protected void enhanceConfiguration(ConfigurationBuilder builder) {
      builder.simpleCache(true);
   }
}
