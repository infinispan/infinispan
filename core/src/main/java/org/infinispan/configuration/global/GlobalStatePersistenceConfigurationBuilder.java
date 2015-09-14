package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.GlobalStatePersistenceConfiguration.ENABLED;
import static org.infinispan.configuration.global.GlobalStatePersistenceConfiguration.LOCATION;

import java.lang.invoke.MethodHandles;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * GlobalStatePersistenceConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class GlobalStatePersistenceConfigurationBuilder extends AbstractGlobalConfigurationBuilder
      implements Builder<GlobalStatePersistenceConfiguration> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final AttributeSet attributes;

   GlobalStatePersistenceConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = GlobalStatePersistenceConfiguration.attributeDefinitionSet();
   }

   public GlobalStatePersistenceConfigurationBuilder enable() {
      return enabled(true);
   }

   public GlobalStatePersistenceConfigurationBuilder disable() {
      return enabled(false);
   }

   public GlobalStatePersistenceConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   public GlobalStatePersistenceConfigurationBuilder location(String location) {
      attributes.attribute(LOCATION).set(location);
      return this;
   }

   @Override
   public void validate() {
      if (attributes.attribute(ENABLED).get() && attributes.attribute(LOCATION).isNull()) {
         throw log.missingGlobalPersistentStateLocation();
      }
   }

   @Override
   public GlobalStatePersistenceConfiguration create() {
      return new GlobalStatePersistenceConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(GlobalStatePersistenceConfiguration template) {

      return this;
   }

}
