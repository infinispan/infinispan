package org.infinispan.factories;

import org.infinispan.commons.marshall.StreamAwareMarshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.impl.ComponentAlias;
import org.infinispan.marshall.core.GlobalMarshaller;
import org.infinispan.marshall.persistence.PersistenceMarshaller;

/**
 * MarshallerFactory.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@DefaultFactoryFor(classes = {StreamingMarshaller.class, StreamAwareMarshaller.class, PersistenceMarshaller.class},
      names = {KnownComponentNames.INTERNAL_MARSHALLER, KnownComponentNames.PERSISTENCE_MARSHALLER})
public class MarshallerFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   private StreamingMarshaller internalMarshaller;
   private StreamAwareMarshaller persistenceMarshaller;

   @Override
   public Object construct(String componentName) {

      if (componentName.equals(StreamingMarshaller.class.getName())) {
         return ComponentAlias.of(KnownComponentNames.INTERNAL_MARSHALLER);
      } else if (componentName.equals(PersistenceMarshaller.class.getName())) {
         return ComponentAlias.of(KnownComponentNames.PERSISTENCE_MARSHALLER);
      }

      switch (componentName) {
         case KnownComponentNames.PERSISTENCE_MARSHALLER:
            if (persistenceMarshaller == null) {
               persistenceMarshaller = new PersistenceMarshaller();
            }
            return persistenceMarshaller;
         case KnownComponentNames.INTERNAL_MARSHALLER:
            if (internalMarshaller == null) {
               internalMarshaller = new GlobalMarshaller();
            }
            return internalMarshaller;
         default:
            throw new IllegalArgumentException(String.format("Marshaller name '%s' not recognised", componentName));
      }
   }
}
