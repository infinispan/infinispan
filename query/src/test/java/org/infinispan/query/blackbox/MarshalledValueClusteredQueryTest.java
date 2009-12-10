package org.infinispan.query.blackbox;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

import java.util.List;

import static org.infinispan.config.Configuration.CacheMode.REPL_SYNC;

/**
 * Clustered version of {@link org.infinispan.query.blackbox.MarshalledValueQueryTest}
 *
 *
 * @author Navin Surtani
 * @since 4.0
 */


@Test (groups = "functional")
public class MarshalledValueClusteredQueryTest extends ClusteredCacheTest {
   
   @Override
   protected void enhanceConfig(Configuration c) {
      c.setUseLazyDeserialization(true);
   }
}
