package org.infinispan.factories;

import static org.infinispan.factories.KnownComponentNames.INTERNAL_MARSHALLER;
import static org.infinispan.factories.KnownComponentNames.USER_MARSHALLER;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.marshall.core.GlobalMarshaller;

/**
 * MarshallerFactory.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@DefaultFactoryFor(classes = {StreamingMarshaller.class, Marshaller.class})
public class MarshallerFactory extends NamedComponentFactory implements AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType) {
      return construct(componentType, INTERNAL_MARSHALLER);
   }

   @Override
   public <T> T construct(Class<T> componentType, String componentName) {

      Marshaller userMarshaller = globalConfiguration.serialization().marshaller();
      Marshaller comp;
      if (componentName.equals(USER_MARSHALLER)) {
         // If userMarshaller is null, then use the old marshaller
         comp = userMarshaller != null ? userMarshaller : new GlobalMarshaller(null);
      } else {
         comp = new GlobalMarshaller(null);
      }
      try {
         return componentType.cast(comp);
      } catch (Exception e) {
         throw new CacheException("Problems casting bootstrap component " + comp.getClass() + " to type " + componentType, e);
      }
   }
}
