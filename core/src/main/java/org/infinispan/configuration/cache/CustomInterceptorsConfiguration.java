package org.infinispan.configuration.cache;

import java.util.List;

import org.infinispan.interceptors.base.CommandInterceptor;

public class CustomInterceptorsConfiguration {
   
   private final List<CommandInterceptor> interceptors;

   CustomInterceptorsConfiguration(List<CommandInterceptor> interceptors) {
      this.interceptors = interceptors;
   }
   
   public List<CommandInterceptor> getInterceptors() {
      return interceptors;
   }

}
