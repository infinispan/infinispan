package org.infinispan.commands.functional.functions;

import org.infinispan.factories.ComponentRegistry;

public interface InjectableComponent {
   void inject(ComponentRegistry registry);

   static void inject(ComponentRegistry componentRegistry, Object... objects) {
      if (componentRegistry == null) {
         return;
      }
      for (Object o : objects) {
         if (o instanceof InjectableComponent) {
            ((InjectableComponent) o).inject(componentRegistry);
         }
      }
   }
}
