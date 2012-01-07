package org.infinispan.configuration.cache;

import java.util.LinkedList;
import java.util.List;

import org.infinispan.interceptors.base.CommandInterceptor;

public class CustomInterceptorsConfigurationBuilder extends AbstractConfigurationChildBuilder<CustomInterceptorsConfiguration> {

   private List<InterceptorConfigurationBuilder> interceptorBuilders = new LinkedList<InterceptorConfigurationBuilder>();
   
   CustomInterceptorsConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }
   
   public InterceptorConfigurationBuilder addInterceptor() {
      InterceptorConfigurationBuilder builder = new InterceptorConfigurationBuilder(this);
      this.interceptorBuilders.add(builder);
      return builder;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   CustomInterceptorsConfiguration create() {
      List<InterceptorConfiguration> interceptors = new LinkedList<InterceptorConfiguration>();
      for (InterceptorConfigurationBuilder builder : interceptorBuilders) {
         interceptors.add(builder.create());
      }
      return new CustomInterceptorsConfiguration(interceptors);
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
