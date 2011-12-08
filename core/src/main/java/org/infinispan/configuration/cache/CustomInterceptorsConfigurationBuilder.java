package org.infinispan.configuration.cache;

import java.util.LinkedList;
import java.util.List;

import org.infinispan.interceptors.base.CommandInterceptor;

public class CustomInterceptorsConfigurationBuilder extends AbstractConfigurationChildBuilder<CustomInterceptorsConfiguration> {

   private List<CommandInterceptor> interceptors = new LinkedList<CommandInterceptor>();
   
   CustomInterceptorsConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }
   
   public CustomInterceptorsConfigurationBuilder addInterceptor(CommandInterceptor interceptor) {
      this.interceptors.add(interceptor);
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   CustomInterceptorsConfiguration create() {
      return new CustomInterceptorsConfiguration(interceptors);
   }
   
   @Override
   public CustomInterceptorsConfigurationBuilder read(CustomInterceptorsConfiguration template) {
      this.interceptors = template.interceptors();
      return this;
   }

}
