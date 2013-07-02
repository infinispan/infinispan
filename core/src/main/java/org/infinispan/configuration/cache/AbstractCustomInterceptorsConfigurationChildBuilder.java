package org.infinispan.configuration.cache;

public abstract class AbstractCustomInterceptorsConfigurationChildBuilder extends AbstractConfigurationChildBuilder {

   private final CustomInterceptorsConfigurationBuilder customInterceptorsBuilder;

   protected AbstractCustomInterceptorsConfigurationChildBuilder(CustomInterceptorsConfigurationBuilder builder) {
      super(builder.getBuilder());
      this.customInterceptorsBuilder = builder;
   }

   protected CustomInterceptorsConfigurationBuilder getCustomInterceptorsBuilder() {
      return customInterceptorsBuilder;
   }

}
