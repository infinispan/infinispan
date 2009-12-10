package org.infinispan.query.blackbox;

import org.infinispan.config.Configuration;
import org.testng.annotations.Test;

/**
 *
 * Test class such that it's possible to run queries on objects with the configuration setUseLazyDeserialization = true
 *
 * @author Navin Surtani
 * @since 4.0
 */


@Test(groups="functional")
public class MarshalledValueQueryTest extends LocalCacheTest {
   @Override
   protected void enhanceConfig(Configuration c) {
      c.setUseLazyDeserialization(true);
   }
}

