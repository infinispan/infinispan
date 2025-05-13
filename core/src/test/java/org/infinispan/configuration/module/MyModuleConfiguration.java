package org.infinispan.configuration.module;

import org.infinispan.commons.configuration.BuiltBy;

@BuiltBy(MyModuleConfigurationBuilder.class)
public class MyModuleConfiguration {
   private final String attribute;

   MyModuleConfiguration(String attribute) {
      this.attribute = attribute;
   }

   public String attribute() {
      return attribute;
   }
}
