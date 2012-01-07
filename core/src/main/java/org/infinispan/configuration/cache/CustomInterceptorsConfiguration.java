package org.infinispan.configuration.cache;

import java.util.List;

public class CustomInterceptorsConfiguration {
   
   private final List<InterceptorConfiguration> interceptors;

   CustomInterceptorsConfiguration(List<InterceptorConfiguration> interceptors) {
      this.interceptors = interceptors;
   }
   
   public List<InterceptorConfiguration> interceptors() {
      return interceptors;
   }

}
