package org.infinispan.configuration.cache;

/**
 *
 * AbstractLoadersConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractLoadersConfigurationChildBuilder extends AbstractConfigurationChildBuilder implements LoadersConfigurationChildBuilder {
   protected AbstractLoadersConfigurationChildBuilder(LoadersConfigurationBuilder builder) {
      super(builder.getBuilder());
   }
}
