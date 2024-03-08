package org.infinispan.configuration.clone;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.testng.annotations.Test;

@Test(testName = "configuration.clone.GlobalTracingConfigCloneTest", groups = "unit")
public class GlobalTracingConfigCloneTest {

   public void test() {
      GlobalConfigurationBuilder originBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      originBuilder.tracing().collectorEndpoint("file://in-memory-local-process");
      assertThat(originBuilder.tracing().enabled()).isTrue();

      GlobalConfiguration original = originBuilder.build();
      assertThat(original.tracing().enabled()).isTrue();

      GlobalConfigurationBuilder clone = GlobalConfigurationBuilder.defaultClusteredBuilder();
      clone.read(original);

      assertThat(clone.tracing().enabled()).isTrue();
   }
}
