package org.infinispan.factories;

import static org.infinispan.util.logging.Log.CONTAINER;

import org.infinispan.commons.time.TimeService;
import org.infinispan.container.versioning.RankCalculator;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.impl.GlobalConfigurationManagerImpl;
import org.infinispan.globalstate.impl.GlobalStateManagerImpl;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistryImpl;
import org.infinispan.remoting.inboundhandler.GlobalInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.RolePermissionMapper;
import org.infinispan.stats.ClusterContainerStats;
import org.infinispan.stats.ContainerStats;
import org.infinispan.stats.impl.ClusterContainerStatsImpl;
import org.infinispan.stats.impl.LocalContainerStatsImpl;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.infinispan.util.EmbeddedTimeService;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.BlockingManagerImpl;
import org.infinispan.util.concurrent.DataOperationOrderer;
import org.infinispan.util.concurrent.NonBlockingManager;
import org.infinispan.util.concurrent.NonBlockingManagerImpl;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLoggerNotifier;
import org.infinispan.util.logging.events.impl.EventLogManagerImpl;
import org.infinispan.util.logging.events.impl.EventLoggerNotifierImpl;
import org.infinispan.xsite.XSiteCacheMapper;
import org.infinispan.xsite.events.NoOpXSiteEventsManager;
import org.infinispan.xsite.events.XSiteEventsManager;
import org.infinispan.xsite.events.XSiteEventsManagerImpl;

/**
 * Factory for building global-scope components which have default empty constructors
 *
 * @author Manik Surtani
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */
@DefaultFactoryFor(classes = {
      ContainerStats.class,
      ClusterContainerStats.class,
      EventLogManager.class,
      InboundInvocationHandler.class, PersistentUUIDManager.class,
      TimeService.class, DataOperationOrderer.class,
      GlobalStateManager.class, GlobalConfigurationManager.class,
      SerializationContextRegistry.class, BlockingManager.class, NonBlockingManager.class,
      RankCalculator.class, EventLoggerNotifier.class, PrincipalRoleMapper.class, RolePermissionMapper.class,
      XSiteCacheMapper.class, XSiteEventsManager.class
})
@Scope(Scopes.GLOBAL)
public class EmptyConstructorFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      if (componentName.equals(InboundInvocationHandler.class.getName()))
         return new GlobalInboundInvocationHandler();
      else if (componentName.equals(TimeService.class.getName()))
         return new EmbeddedTimeService();
      else if (componentName.equals(EventLogManager.class.getName()))
         return new EventLogManagerImpl();
      else if (componentName.equals(PersistentUUIDManager.class.getName()))
         return new PersistentUUIDManagerImpl();
      else if (componentName.equals(GlobalStateManager.class.getName()))
         return new GlobalStateManagerImpl();
      else if (componentName.equals(GlobalConfigurationManager.class.getName()))
         return new GlobalConfigurationManagerImpl();
      else if (componentName.equals(DataOperationOrderer.class.getName()))
         return new DataOperationOrderer();
      else if (componentName.equals(SerializationContextRegistry.class.getName()))
         return new SerializationContextRegistryImpl();
      else if (componentName.equals(BlockingManager.class.getName()))
         return new BlockingManagerImpl();
      else if (componentName.equals(NonBlockingManager.class.getName()))
         return new NonBlockingManagerImpl();
      else if (componentName.equals(RankCalculator.class.getName()))
         return new RankCalculator();
      else if (componentName.equals(EventLoggerNotifier.class.getName()))
         return new EventLoggerNotifierImpl();
      else if (componentName.equals(PrincipalRoleMapper.class.getName())) {
         if (globalConfiguration.security().authorization().enabled()) {
            return globalConfiguration.security().authorization().principalRoleMapper();
         } else {
            return null;
         }
      } else if (componentName.equals(RolePermissionMapper.class.getName())) {
         if (globalConfiguration.security().authorization().enabled()) {
            return globalConfiguration.security().authorization().rolePermissionMapper();
         } else {
            return null;
         }
      } else if (componentName.equals(ContainerStats.class.getName()))
         return new LocalContainerStatsImpl();
      else if (componentName.equals(ClusterContainerStats.class.getName()))
         return new ClusterContainerStatsImpl();
      else if (componentName.equals(XSiteCacheMapper.class.getName())) {
         return new XSiteCacheMapper();
      } else if (componentName.equals(XSiteEventsManager.class.getName())) {
         return globalConfiguration.isClustered() ?
               new XSiteEventsManagerImpl() :
               NoOpXSiteEventsManager.INSTANCE;
      }

      throw CONTAINER.factoryCannotConstructComponent(componentName);
   }
}
