package org.infinispan.factories;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.ReflectionUtil;

/**
 * A specialized type of component factory that knows how to create named components, identified with the {@link
 * org.infinispan.factories.annotations.ComponentName} annotation on the classes requested in {@link
 * org.infinispan.factories.annotations.Inject} annotated methods.
 *
 * @author Manik Surtani
 * @since 4.0
 * @deprecated Since 9.4, please implement {@link AbstractComponentFactory#construct(String)} directly.
 */
@Deprecated
public abstract class NamedComponentFactory extends AbstractComponentFactory {

   @Override
   public Object construct(String componentName) {
      Class<?> componentType;
      try {
         componentType = ReflectionUtil.getClassForName(componentName, globalComponentRegistry.getClassLoader());
      } catch (ClassNotFoundException e) {
         throw new CacheConfigurationException(e);
      }
      return construct(componentType, componentName);
   }

   /**
    * Constructs a component.
    *
    * @param componentType type of component
    * @return a component
    */
   public abstract <T> T construct(Class<T> componentType, String componentName);
}
