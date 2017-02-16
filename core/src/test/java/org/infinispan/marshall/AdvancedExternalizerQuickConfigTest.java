package org.infinispan.marshall;

import java.util.Map;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests configuration of user defined {@link AdvancedExternalizer} implementations
 * using helpers methods in {@link GlobalConfigurationBuilder}.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "marshall.AdvancedExternalizerQuickConfigTest")
public class AdvancedExternalizerQuickConfigTest extends AdvancedExternalizerTest {

   @Override
   protected GlobalConfigurationBuilder createForeignExternalizerGlobalConfig() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder().clusteredDefault();
      builder.serialization()
         .addAdvancedExternalizer(1234, new IdViaConfigObj.Externalizer())
         .addAdvancedExternalizer(new IdViaAnnotationObj.Externalizer())
         .addAdvancedExternalizer(3456, new IdViaBothObj.Externalizer());
      return builder;
   }

   public void testExternalizerConfigInfo() {
      Map<Integer, AdvancedExternalizer<?>> advExts =
            manager(0).getCacheManagerConfiguration().serialization().advancedExternalizers();
      assert advExts.size() == 3;
      AdvancedExternalizer<?> ext = advExts.get(1234);
      assert ext != null;
      assert ext.getClass() == IdViaConfigObj.Externalizer.class;
      ext = advExts.get(5678);
      assert ext != null;
      assert ext.getClass() == IdViaAnnotationObj.Externalizer.class;
      assert ext.getId() == 5678;
      ext = advExts.get(3456);
      assert ext != null;
      assert ext.getClass() == IdViaBothObj.Externalizer.class;
   }

   @Override
   protected String getCacheName() {
      return "ForeignExternalizersQuickConfig";
   }

}
