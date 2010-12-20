package org.infinispan.marshall;

import org.infinispan.config.ExternalizerConfig;
import org.infinispan.config.GlobalConfiguration;
import org.testng.annotations.Test;

import java.util.List;

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

   public void testExternalizerConfigInfo() {
      List<ExternalizerConfig> externalizers = manager(0).getGlobalConfiguration().getExternalizersType().getExternalizerConfigs();
      assert externalizers.size() == 3;
      ExternalizerConfig config = externalizers.get(0);
      assert config.getExternalizer() != null;
      assert config.getExternalizerClass() == IdViaConfigObj.Externalizer.class.getName();
      assert config.getId() == 1234;
      config = externalizers.get(1);
      assert config.getExternalizer() != null;
      assert config.getExternalizerClass() == IdViaAnnotationObj.Externalizer.class.getName();
      assert config.getId() == 5678;
      config = externalizers.get(2);
      assert config.getExternalizer() != null;
      assert config.getExternalizerClass() == IdViaBothObj.Externalizer.class.getName();
      assert config.getId() == 3456;
   }

   @Override
   protected String getCacheName() {
      return "ForeignExternalizersQuickConfig";
   }

}
