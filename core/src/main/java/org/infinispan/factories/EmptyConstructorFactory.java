package org.infinispan.factories;

import org.infinispan.commands.CancellationService;
import org.infinispan.commands.CancellationServiceImpl;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.ExternalizerTable;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.InboundInvocationHandlerImpl;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.topology.ClusterTopologyManagerImpl;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.LocalTopologyManagerImpl;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.TimeService;
import org.infinispan.xsite.BackupReceiverRepository;
import org.infinispan.xsite.BackupReceiverRepositoryImpl;

/**
 * Factory for building global-scope components which have default empty constructors
 *
 * @author Manik Surtani
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */

@DefaultFactoryFor(classes = {InboundInvocationHandler.class, RemoteCommandsFactory.class, ExternalizerTable.class,
                              BackupReceiverRepository.class, CancellationService.class, TimeService.class})
@Scope(Scopes.GLOBAL)
public class EmptyConstructorFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (componentType.equals(InboundInvocationHandler.class))
         return (T) new InboundInvocationHandlerImpl();
      else if (componentType.equals(RemoteCommandsFactory.class))
         return (T) new RemoteCommandsFactory();
      else if (componentType.equals(ExternalizerTable.class))
         return (T) new ExternalizerTable();
      else if (componentType.equals(LocalTopologyManager.class))
         return (T) new LocalTopologyManagerImpl();
      else if (componentType.equals(ClusterTopologyManager.class))
         return (T) new ClusterTopologyManagerImpl();
      else if (componentType.equals(BackupReceiverRepository.class))
         return (T) new BackupReceiverRepositoryImpl();
      else if (componentType.equals(CancellationService.class))
         return (T) new CancellationServiceImpl();
      else if (componentType.equals(TimeService.class)) {
         return (T) new DefaultTimeService();
      }

      throw new CacheConfigurationException("Don't know how to create a " + componentType.getName());
   }
}
