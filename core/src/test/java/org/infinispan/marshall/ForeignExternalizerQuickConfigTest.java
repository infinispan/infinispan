package org.infinispan.marshall;

import org.infinispan.config.GlobalConfiguration;
import org.testng.annotations.Test;

/**
 * Tests configuration of user defined {@link Externalizer} implementations
 * using helpers methods in {@link GlobalConfiguration}.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "marshall.ForeignExternalizerQuickConfigTest")
public class ForeignExternalizerQuickConfigTest extends ForeignExternalizerTest {

   @Override
   protected GlobalConfiguration createForeignExternalizerGlobalConfig() {
      GlobalConfiguration globalCfg = GlobalConfiguration.getClusteredDefault();
      globalCfg.addExternalizer(1234, new IdViaConfigObj.Externalizer());
      globalCfg.addExternalizer(new IdViaAnnotationObj.Externalizer());
      globalCfg.addExternalizer(3456, new IdViaBothObj.Externalizer());
      return globalCfg;
   }

   @Override
   protected String getCacheName() {
      return "ForeignExternalizersQuickConfig";
   }

}
