package org.infinispan.factories;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Factory that creates components used internally within Infinispan, and also wires dependencies into the components.
 * <p>
 * The {@link InternalCacheFactory} is a special subclass of this, which bootstraps the construction of other
 * components. When this class is loaded, it maintains a static list of known default factories for known components,
 * which it then delegates to, when actually performing the construction.
 * <p>
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @see Inject
 * @see ComponentRegistry
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public abstract class AbstractComponentFactory implements ComponentFactory {
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

   protected void assertTypeConstructable(Class<?> requestedType, Class<?>... ableToConstruct) {
      boolean canConstruct = false;
      for (Class<?> c : ableToConstruct) {
         canConstruct = canConstruct || requestedType.isAssignableFrom(c);
      }
      if (!canConstruct) throw log.factoryCannotConstructComponent(requestedType.getName());
   }

}
