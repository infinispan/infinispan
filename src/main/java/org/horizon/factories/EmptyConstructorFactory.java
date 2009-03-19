package org.horizon.factories;

import org.horizon.commands.RemoteCommandFactory;
import org.horizon.config.ConfigurationException;
import org.horizon.factories.annotations.DefaultFactoryFor;
import org.horizon.factories.scopes.Scope;
import org.horizon.factories.scopes.Scopes;
import org.horizon.marshall.Marshaller;
import org.horizon.marshall.VersionAwareMarshaller;
import org.horizon.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.horizon.remoting.InboundInvocationHandler;

/**
 * Factory for building global-scope components which have default empty constructors
 *
 * @author Manik Surtani
 * @since 1.0
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
