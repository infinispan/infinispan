package org.infinispan.factories;

import org.infinispan.commands.RemoteCommandFactory;
import org.infinispan.config.ConfigurationException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.InboundInvocationHandler;

/**
 * Factory for building global-scope components which have default empty constructors
 *
 * @author Manik Surtani
 * @since 4.0
 */
@DefaultFactoryFor(classes = {InboundInvocationHandler.class, CacheManagerNotifier.class, Marshaller.class, RemoteCommandFactory.class})
@Scope(Scopes.GLOBAL)
public class EmptyConstructorFactory extends AbstractComponentFactory implements AutoInstantiableFactory {
   public <T> T construct(Class<T> componentType) {
      try {
         if (componentType.isInterface()) {
            Class componentImpl;
            if (componentType.equals(Marshaller.class)) {
               componentImpl = VersionAwareMarshaller.class;
            } else {
               // add an "Impl" to the end of the class name and try again
               componentImpl = getClass().getClassLoader().loadClass(componentType.getName() + "Impl");
            }
            return componentType.cast(componentImpl.newInstance());
         } else {
            return componentType.newInstance();
         }
      }
      catch (Exception e) {
         throw new ConfigurationException("Unable to create component " + componentType, e);
      }

   }
}
