package org.infinispan.factories;

import org.infinispan.commands.CancellationService;
import org.infinispan.commands.CancellationServiceImpl;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.InvocationContextContainerImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.ExternalizerTable;
import org.infinispan.remoting.inboundhandler.GlobalInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.impl.EventLogManagerImpl;
import org.infinispan.xsite.BackupReceiverRepository;
import org.infinispan.xsite.BackupReceiverRepositoryImpl;

/**
 * Factory for building global-scope components which have default empty constructors
 *
 * @author Manik Surtani
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */

@DefaultFactoryFor(classes = {BackupReceiverRepository.class, CancellationService.class, EventLogManager.class,
                              ExternalizerTable.class, InboundInvocationHandler.class, PersistentUUIDManager.class,
                              InvocationContextContainer.class,
                              RemoteCommandsFactory.class, TimeService.class})
@Scope(Scopes.GLOBAL)
public class EmptyConstructorFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (componentType.equals(BackupReceiverRepository.class))
         return (T) new BackupReceiverRepositoryImpl();
      else if (componentType.equals(CancellationService.class))
         return (T) new CancellationServiceImpl();
      else if (componentType.equals(ExternalizerTable.class))
         return (T) new ExternalizerTable();
      else if (componentType.equals(InboundInvocationHandler.class))
         return (T) new GlobalInboundInvocationHandler();
      else if (componentType.equals(RemoteCommandsFactory.class))
         return (T) new RemoteCommandsFactory();
      else if (componentType.equals(TimeService.class))
         return (T) new DefaultTimeService();
      else if (componentType.equals(EventLogManager.class))
         return (T) new EventLogManagerImpl();
      else if (componentType.equals(PersistentUUIDManager.class))
         return (T) new PersistentUUIDManagerImpl();
      else if (componentType.equals(InvocationContextContainer.class))
         return (T) new InvocationContextContainerImpl();

      throw new CacheConfigurationException("Don't know how to create a " + componentType.getName());
   }
}
