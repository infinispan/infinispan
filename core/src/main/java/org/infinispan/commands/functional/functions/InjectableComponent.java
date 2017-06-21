package org.infinispan.commands.functional.functions;

import org.infinispan.factories.ComponentRegistry;

public interface InjectableComponent {
   void inject(ComponentRegistry registry);
}
