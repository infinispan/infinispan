package org.infinispan.configuration.cache;

import java.util.Collections;
import java.util.List;

/**
 * Configures custom interceptors to be added to the cache.
 * 
 * @author pmuir
 */
public class CustomInterceptorsConfiguration {

   private final List<InterceptorConfiguration> interceptors;

   CustomInterceptorsConfiguration(List<InterceptorConfiguration> interceptors) {
      this.interceptors = interceptors;
   }

   public CustomInterceptorsConfiguration() {
      this.interceptors = Collections.emptyList();
   }

   /**
    * This specifies a list of {@link InterceptorConfiguration} instances to be referenced when building the interceptor
    * chain.
    * @return A list of {@link InterceptorConfiguration}s. May be an empty list, will never be null.
    */
   public List<InterceptorConfiguration> interceptors() {
      return interceptors;
   }

   @Override
   public String toString() {
      return "CustomInterceptorsConfiguration{" +
            "interceptors=" + interceptors +
            '}';
   }

}
