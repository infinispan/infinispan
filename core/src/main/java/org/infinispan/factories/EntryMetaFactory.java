package org.infinispan.factories;

import org.infinispan.container.EntryFactory;
import org.infinispan.container.EntryFactoryImpl;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;

@DefaultFactoryFor(classes = {EntryFactory.class, InternalEntryFactory.class})
public class EntryMetaFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {

      if (componentType.equals(EntryFactory.class)) {
         return (T) new EntryFactoryImpl();
      } else {
         return (T) new InternalEntryFactoryImpl();
      }
   }
}
