package org.infinispan.server.insights;

import java.util.function.Supplier;

import com.redhat.insights.config.EnvAndSysPropsInsightsConfiguration;

public class InfinispanInsightsConfiguration extends EnvAndSysPropsInsightsConfiguration {

   private final Supplier<String> identificationName;

   public InfinispanInsightsConfiguration(Supplier<String> identificationName) {
      this.identificationName = identificationName;
   }

   @Override
   public String getIdentificationName() {
      return identificationName.get();
   }
}
