package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.marshall.core.CacheMarshaller;
import org.infinispan.marshall.core.GlobalMarshaller;
import org.infinispan.marshall.core.VersionAwareMarshaller;

import static org.infinispan.factories.KnownComponentNames.*;

/**
 * MarshallerFactory.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@DefaultFactoryFor(classes = {StreamingMarshaller.class, Marshaller.class})
public class MarshallerFactory extends NamedComponentFactory implements AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType, String componentName) {
      Object comp;
      Marshaller configMarshaller =
            globalConfiguration.serialization().marshaller();
      boolean isVersionAwareMarshaller =
            configMarshaller instanceof VersionAwareMarshaller;

      if (isVersionAwareMarshaller) {
         if (componentName.equals(GLOBAL_MARSHALLER))
            comp = new GlobalMarshaller((VersionAwareMarshaller) configMarshaller);
         else if (componentName.equals(CACHE_MARSHALLER))
            comp = new CacheMarshaller(new VersionAwareMarshaller());
         else
            throw new CacheException("Don't know how to handle type " + componentType);
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
