package org.infinispan.server.rollingupgrade;

import org.infinispan.server.cli.XSiteCliOperations;
import org.infinispan.server.functional.XSiteIT;
import org.infinispan.server.functional.hotrod.XSiteHotRodCacheOperations;
import org.infinispan.server.functional.rest.XSiteRestCacheOperations;
import org.infinispan.server.functional.rest.XSiteRestMetricsOperations;
import org.infinispan.server.test.junit5.InfinispanSuite;
import org.infinispan.server.test.junit5.RollingUpgradeHandlerXSiteExtension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Cross-Site suite for rolling upgrade tests
 *
 * @author William Burns
 * @since 16.0
 */
@Suite(failIfNoTests = false)
@SelectClasses({
      XSiteRestMetricsOperations.class,
      XSiteHotRodCacheOperations.class,
      XSiteRestCacheOperations.class,
      XSiteCliOperations.class
})
public class RollingUpgradeXSiteIT extends InfinispanSuite {

   @RegisterExtension
   public static final RollingUpgradeHandlerXSiteExtension SERVERS = RollingUpgradeHandlerXSiteExtension.from(
         RollingUpgradeXSiteIT.class, XSiteIT.EXTENSION_BUILDER, "15.2.0.Final", "15.2.1.Final");

}
