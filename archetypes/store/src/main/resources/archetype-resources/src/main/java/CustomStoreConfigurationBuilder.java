package ${package};

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class CustomStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<CustomStoreConfiguration, CustomStoreConfigurationBuilder> {

   public CustomStoreConfigurationBuilder(
         PersistenceConfigurationBuilder builder) {
      super(builder, CustomStoreConfiguration.attributeDefinitionSet());
      // TODO Auto-generated constructor stub
   }

   public CustomStoreConfigurationBuilder sampleAttribute(String sampleAttribute) {
      // TODO Auto-generated method stub
      attributes.attribute(CustomStoreConfiguration.SAMPLE_ATTRIBUTE).set(sampleAttribute);
      return this;
   }

   @Override
   public void validate() {
      super.validate();
      /*
       * Perform any validation checks required. Throwing a Runtime Exception will prevent your store from being
       * created and cache startup will fail.
       *
       * The call to <code>super.validate();</code> should be kept to ensure that the attributes inherited from
       * {@link AbstractStoreConfigurationBuilder} are still validated. If no additional validation is required by your
       * store implementation, then this overridden method can be removed.
       */
   }

   @Override
   public CustomStoreConfiguration create() {
      return new CustomStoreConfiguration(attributes.protect(), async.create());
   }

   @Override
   public CustomStoreConfigurationBuilder self() {
      return this;
   }
}
