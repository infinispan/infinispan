package org.infinispan.configuration.cache;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Configures custom interceptors to be added to the cache.
 *
 * @author pmuir
 * @deprecated Since 10.0, custom interceptors support will be removed and only modules will be able to define interceptors
 */
@Deprecated
public class CustomInterceptorsConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<CustomInterceptorsConfiguration> {

   private List<InterceptorConfigurationBuilder> interceptorBuilders = new LinkedList<>();

   CustomInterceptorsConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   /**
    * Adds a new custom interceptor definition, to be added to the cache when the cache is started.
    * @deprecated Since 10.0, custom interceptors support will be removed and only modules will be able to define interceptors
    */
   @Deprecated
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
         List<InterceptorConfiguration> interceptors = new ArrayList<>(interceptorBuilders.size());
         for (InterceptorConfigurationBuilder builder : interceptorBuilders) interceptors.add(builder.create());
         return new CustomInterceptorsConfiguration(interceptors);
      }
   }

   @Override
   public CustomInterceptorsConfigurationBuilder read(CustomInterceptorsConfiguration template, Combine combine) {
      this.interceptorBuilders = new LinkedList<>();
      for (InterceptorConfiguration c : template.interceptors()) {
         this.interceptorBuilders.add(new InterceptorConfigurationBuilder(this).read(c, combine));
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
