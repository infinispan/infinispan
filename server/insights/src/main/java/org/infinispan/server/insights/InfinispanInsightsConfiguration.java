package org.infinispan.server.insights;

import java.util.function.Supplier;

import com.redhat.insights.config.EnvAndSysPropsInsightsConfiguration;

public class InfinispanInsightsConfiguration extends EnvAndSysPropsInsightsConfiguration {

   public static final String ENV_RHEL_MACHINE_ID_FILE_PATH = "RHT_INSIGHTS_RHEL_MACHINE_ID_FILE_PATH";

   private final Supplier<String> identificationName;

   public InfinispanInsightsConfiguration(Supplier<String> identificationName) {
      this.identificationName = identificationName;
   }

   @Override
   public String getIdentificationName() {
      return identificationName.get();
   }

   @Override
   public String getMachineIdFilePath() {
      String value = lookup(ENV_RHEL_MACHINE_ID_FILE_PATH);
      if (value != null) {
         return value;
      }
      return super.getMachineIdFilePath();
   }

   private String lookup(String env) {
      String value = System.getenv(env);
      if (value == null) {
         value = System.getProperty(env.toLowerCase().replace('_', '.'));
      }
      return value;
   }
}
