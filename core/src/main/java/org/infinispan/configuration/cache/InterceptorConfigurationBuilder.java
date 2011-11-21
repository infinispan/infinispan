package org.infinispan.configuration.cache;

import org.infinispan.configuration.cache.InterceptorConfiguration.Position;
import org.infinispan.interceptors.base.CommandInterceptor;

public class InterceptorConfigurationBuilder extends AbstractCustomInterceptorsConfigurationChildBuilder<InterceptorConfiguration> {

   private Class<? extends CommandInterceptor> after;
   private Class<? extends CommandInterceptor> before;
   private CommandInterceptor interceptor;
   private int index;
   private Position position;
   
   InterceptorConfigurationBuilder(CustomInterceptorsConfigurationBuilder builder) {
      super(builder);
   }

   public InterceptorConfigurationBuilder after(Class<? extends CommandInterceptor> after) {
      this.after = after;
      return this;
   }

   public InterceptorConfigurationBuilder before(Class<? extends CommandInterceptor> before) {
      this.before = before;
      return this;
   }

   public InterceptorConfigurationBuilder interceptor(CommandInterceptor interceptor) {
      this.interceptor = interceptor;
      return this;
   }

   public InterceptorConfigurationBuilder index(int i) {
      this.index = i;
      return this;
   }

   public InterceptorConfigurationBuilder position(Position p) {
      this.position = p;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub

   }

   @Override
   InterceptorConfiguration create() {
      return new InterceptorConfiguration(after, before, interceptor, index, position);
   }

}
