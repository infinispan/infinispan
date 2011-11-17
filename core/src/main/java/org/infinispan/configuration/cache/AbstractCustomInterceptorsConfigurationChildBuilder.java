package org.infinispan.configuration.cache;

public abstract class AbstractCustomInterceptorsConfigurationChildBuilder<T> extends AbstractConfigurationChildBuilder<T> {

   private final CustomInterceptorsConfigurationBuilder customInterceptorsBuilder;
   
   protected AbstractCustomInterceptorsConfigurationChildBuilder(CustomInterceptorsConfigurationBuilder builder) {
      super(builder.getBuilder());
      this.customInterceptorsBuilder = builder; 
   }
   
   protected CustomInterceptorsConfigurationBuilder getCustomInterceptorsBuilder() {
      return customInterceptorsBuilder;
   }
   
}
