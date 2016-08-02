package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.marshall.core.GlobalMarshaller;

/**
 * MarshallerFactory.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@DefaultFactoryFor(classes = {StreamingMarshaller.class, Marshaller.class})
public class MarshallerFactory extends EmptyConstructorFactory implements AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType) {
      Object comp;
      Marshaller configMarshaller =
            globalConfiguration.serialization().marshaller();

      if (configMarshaller == null) {
         comp = new GlobalMarshaller();
      } else {
         comp = configMarshaller;
      }

      try {
         return componentType.cast(comp);
      } catch (Exception e) {
         throw new CacheException("Problems casting bootstrap component " + comp.getClass() + " to type " + componentType, e);
      }
   }

}
