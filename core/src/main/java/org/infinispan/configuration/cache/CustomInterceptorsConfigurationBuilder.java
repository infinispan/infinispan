package org.infinispan.configuration.cache;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Configures custom interceptors to be added to the cache.
 *
 * @author pmuir
 *
 */
public class CustomInterceptorsConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<CustomInterceptorsConfiguration> {

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
   public void validate() {
      for (InterceptorConfigurationBuilder builder : interceptorBuilders) builder.validate();
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      for (InterceptorConfigurationBuilder builder : interceptorBuilders) builder.validate(globalConfig);
   }

   @Override
   public CustomInterceptorsConfiguration create() {
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

   @Override
   public String toString() {
      return "CustomInterceptorsConfigurationBuilder{" +
            "interceptors=" + interceptorBuilders +
            '}';
   }
}
