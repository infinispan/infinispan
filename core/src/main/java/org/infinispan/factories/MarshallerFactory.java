package org.infinispan.factories;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.impl.ComponentAlias;
import org.infinispan.marshall.core.GlobalMarshaller;

/**
 * MarshallerFactory.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@DefaultFactoryFor(classes = {StreamingMarshaller.class, Marshaller.class})
public class MarshallerFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      if (componentName.equals(Marshaller.class.getName())) {
         return ComponentAlias.of(StreamingMarshaller.class);
      }

      Marshaller configMarshaller = globalConfiguration.serialization().marshaller();
      return new GlobalMarshaller(configMarshaller);
   }
}
