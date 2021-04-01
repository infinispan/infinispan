package org.infinispan.cloudevents.impl;

import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Use a factory for {@link KafkaEventSender} so that tests can replace it with a mock
 * using {@code org.infinispan.commands.module.TestGlobalConfigurationBuilder}.
 *
 * @author Dan Berindei
 * @since 12
 */
@Scope(Scopes.GLOBAL)
@DefaultFactoryFor(classes = KafkaEventSender.class)
public class KafkaEventSenderFactory implements ComponentFactory, AutoInstantiableFactory {
   @Override
   public Object construct(String componentName) {
      return new KafkaEventSenderImpl();
   }
}
