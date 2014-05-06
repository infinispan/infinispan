package org.infinispan.it.osgi;

import static org.infinispan.it.osgi.util.IspnKarafOptions.perSuiteOptions;
import static org.ops4j.pax.exam.CoreOptions.options;

import org.infinispan.test.MultipleCacheManagersTest;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;

/**
 * @author mgencur
 * @author isavin
 */
public abstract class BaseInfinispanCoreOSGiTest extends MultipleCacheManagersTest {
   @Configuration
   public Option[] config() throws Exception {
      return options(perSuiteOptions());
   }
}
