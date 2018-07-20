package org.infinispan.factories;

import org.infinispan.commons.marshall.StreamAwareMarshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.impl.ComponentAlias;
import org.infinispan.marshall.core.GlobalMarshaller;
import org.infinispan.marshall.persistence.impl.PersistenceMarshallerImpl;

/**
 * MarshallerFactory.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@DefaultFactoryFor(classes = {StreamingMarshaller.class, StreamAwareMarshaller.class},
      names = {KnownComponentNames.INTERNAL_MARSHALLER, KnownComponentNames.PERSISTENCE_MARSHALLER})
public class MarshallerFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {

      if (componentName.equals(StreamingMarshaller.class.getName())) {
         return ComponentAlias.of(KnownComponentNames.INTERNAL_MARSHALLER);
      }

      switch (componentName) {
         case KnownComponentNames.PERSISTENCE_MARSHALLER:
            return new PersistenceMarshallerImpl();
         case KnownComponentNames.INTERNAL_MARSHALLER:
            return new GlobalMarshaller();
         default:
            throw new IllegalArgumentException(String.format("Marshaller name '%s' not recognised", componentName));
      }
   }
}
