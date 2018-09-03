package org.infinispan.factories;

import org.infinispan.commands.CancellationService;
import org.infinispan.commands.CancellationServiceImpl;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.impl.GlobalConfigurationManagerImpl;
import org.infinispan.globalstate.impl.GlobalStateManagerImpl;
import org.infinispan.remoting.inboundhandler.GlobalInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.stream.impl.IteratorHandler;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.infinispan.util.EmbeddedTimeService;
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
                              InboundInvocationHandler.class, PersistentUUIDManager.class,
                              RemoteCommandsFactory.class, TimeService.class,
                              IteratorHandler.class, GlobalStateManager.class, GlobalConfigurationManager.class})

@Scope(Scopes.GLOBAL)
public class EmptyConstructorFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public Object construct(String componentName) {
      if (componentName.equals(BackupReceiverRepository.class.getName()))
         return new BackupReceiverRepositoryImpl();
      else if (componentName.equals(CancellationService.class.getName()))
         return new CancellationServiceImpl();
      else if (componentName.equals(InboundInvocationHandler.class.getName()))
         return new GlobalInboundInvocationHandler();
      else if (componentName.equals(RemoteCommandsFactory.class.getName()))
         return new RemoteCommandsFactory();
      else if (componentName.equals(TimeService.class.getName()))
         return new EmbeddedTimeService();
      else if (componentName.equals(EventLogManager.class.getName()))
         return new EventLogManagerImpl();
      else if (componentName.equals(PersistentUUIDManager.class.getName()))
         return new PersistentUUIDManagerImpl();
      else if (componentName.equals(IteratorHandler.class.getName()))
         return new IteratorHandler();
      else if (componentName.equals(GlobalStateManager.class.getName()))
         return new GlobalStateManagerImpl();
      else if (componentName.equals(GlobalConfigurationManager.class.getName()))
         return new GlobalConfigurationManagerImpl();

      throw log.factoryCannotConstructComponent(componentName);
   }
}
