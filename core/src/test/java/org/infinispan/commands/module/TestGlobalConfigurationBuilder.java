package org.infinispan.commands.module;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

/**
 * A {@link Builder} implementation of {@link TestGlobalConfiguration}.
 *
 * @author Dan Berindei
 * @see TestGlobalConfiguration
 * @since 9.4
 */
public class TestGlobalConfigurationBuilder implements Builder<TestGlobalConfiguration> {

   private final AttributeSet attributes;

   public TestGlobalConfigurationBuilder(GlobalConfigurationBuilder builder) {
      this.attributes = TestGlobalConfiguration.attributeSet();
   }

   public TestGlobalConfigurationBuilder testComponent(String componentName, Object instance) {
      this.attributes.attribute(TestGlobalConfiguration.TEST_COMPONENTS).get().put(componentName, instance);
      return this;
   }

   @Override
   public void validate() {
      //nothing
   }

   @Override
   public TestGlobalConfiguration create() {
      return new TestGlobalConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(TestGlobalConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "TestGlobalConfigurationBuilder [attributes=" + attributes + ']';
   }
}
