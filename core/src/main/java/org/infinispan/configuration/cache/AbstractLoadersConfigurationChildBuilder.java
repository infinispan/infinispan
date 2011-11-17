package org.infinispan.configuration.cache;

public abstract class AbstractLoadersConfigurationChildBuilder<T> extends AbstractConfigurationChildBuilder<T> implements LoadersConfigurationChildBuilder {

   private final LoadersConfigurationBuilder builder;
   
   protected AbstractLoadersConfigurationChildBuilder(LoadersConfigurationBuilder builder) {
      super(builder.getBuilder());
      this.builder = builder; 
   }
   
   protected LoadersConfigurationBuilder getLoadersBuilder() {
      return builder;
   }
   
}
