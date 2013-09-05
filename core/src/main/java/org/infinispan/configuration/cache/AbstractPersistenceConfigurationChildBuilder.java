package org.infinispan.configuration.cache;

/**
 *
 * AbstractPersistenceConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractPersistenceConfigurationChildBuilder extends AbstractConfigurationChildBuilder implements PersistenceConfigurationChildBuilder {
   protected AbstractPersistenceConfigurationChildBuilder(PersistenceConfigurationBuilder builder) {
      super(builder.getBuilder());
   }
}
