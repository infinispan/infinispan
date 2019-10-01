package org.infinispan.factories;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

@Scope(Scopes.NONE)
public class AnyScopeComponentFactory implements ComponentFactory {
   protected static final Log log = LogFactory.getLog(AbstractComponentFactory.class);
   @Inject protected GlobalComponentRegistry globalComponentRegistry;
   @Inject protected GlobalConfiguration globalConfiguration;

   @Override
   public Object construct(String name) {
      Class<?> componentType;
      try {
         componentType = ReflectionUtil.getClassForName(name, globalComponentRegistry.getClassLoader());
      } catch (ClassNotFoundException e) {
         throw new CacheConfigurationException(e);
      }
      return construct(componentType);
   }

   /**
    * Constructs a component.
    *
    * @param componentType type of component
    * @return a component
    * @deprecated Since 9.4, please override {@link ComponentFactory#construct(String)} instead.
    */
   @Deprecated
   public <T> T construct(Class<T> componentType) {
      throw new UnsupportedOperationException();
   }
}
