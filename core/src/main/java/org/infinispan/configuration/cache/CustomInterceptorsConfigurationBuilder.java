package org.infinispan.configuration.cache;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Configures custom interceptors to be added to the cache.
 * 
 * @author pmuir
 *
 */
public class CustomInterceptorsConfigurationBuilder extends AbstractConfigurationChildBuilder<CustomInterceptorsConfiguration> {

   private List<InterceptorConfigurationBuilder> interceptorBuilders = new LinkedList<InterceptorConfigurationBuilder>();
   
   CustomInterceptorsConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Adds a new custom interceptor definition, to be added to the cache when the cache is started.
    */
   public InterceptorConfigurationBuilder addInterceptor() {
      InterceptorConfigurationBuilder builder = new InterceptorConfigurationBuilder(this);
      this.interceptorBuilders.add(builder);
      return builder;
   }

   @Override
   void validate() {
      // Nothing to validate
   }

   @Override
   CustomInterceptorsConfiguration create() {
      if (interceptorBuilders.isEmpty()) {
         return new CustomInterceptorsConfiguration();
      } else {
         List<InterceptorConfiguration> interceptors = new ArrayList<InterceptorConfiguration>(interceptorBuilders.size());
         for (InterceptorConfigurationBuilder builder : interceptorBuilders) interceptors.add(builder.create());
         return new CustomInterceptorsConfiguration(interceptors);
      }
   }
   
   @Override
   public CustomInterceptorsConfigurationBuilder read(CustomInterceptorsConfiguration template) {
      this.interceptorBuilders = new LinkedList<InterceptorConfigurationBuilder>();
      for (InterceptorConfiguration c : template.interceptors()) {
         this.interceptorBuilders.add(new InterceptorConfigurationBuilder(this).read(c));
      }
      return this;
   }
}
