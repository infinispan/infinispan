package org.infinispan.configuration.cache;

/**
 * @deprecated Since 10.0, custom interceptors support will be removed and only modules will be able to define interceptors
 */
@Deprecated
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
