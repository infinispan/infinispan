package org.infinispan.configuration.module;

import org.infinispan.commons.configuration.BuiltBy;

@BuiltBy(MyModuleConfigurationBuilder.class)
public class MyModuleConfiguration {
   final private String attribute;

   MyModuleConfiguration(String attribute) {
      this.attribute = attribute;
   }

   public String attribute() {
      return attribute;
   }
}
