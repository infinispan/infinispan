package org.infinispan.spring;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.jmx.MBeanServerLookup;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.commons.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Properties;

/**
 * Helper class similar to {@link ConfigurationOverrides}. As the name suggests, this class is for overriding a
 * {@link org.infinispan.configuration.global.GlobalConfiguration}.
 *
 * @author navssurtani
 */
public class GlobalConfigurationOverrides {

   private final Log logger = LogFactory.getLog(getClass());

   private Boolean exposeGlobalJmxStatistics;

   private Properties mBeanServerProperties;

   private String jmxDomain;

   private String mBeanServerLookupClass;

   private MBeanServerLookup mBeanServerLookup;

   private Boolean allowDuplicateDomains;

   private String cacheManagerName;

   private String clusterName;

   private String machineId;

   private String rackId;

   private String siteId;

   private Boolean strictPeerToPeer;

   private Long distributedSyncTimeout;

   private String transportClass;

   private String transportNodeName;

   private String asyncListenerExecutorFactoryClass;

   private String asyncTransportExecutorFactoryClass;

   private String remoteCommandsExecutorFactoryClass;

   private String evictionScheduledExecutorFactoryClass;

   private String replicationQueueScheduledExecutorFactoryClass;

   private String marshallerClass;

   private Properties transportProperties;

   private String shutdownHookBehavior;

   private Properties asyncListenerExecutorProperties;

   private Properties asyncTransportExecutorProperties;

   private Properties remoteCommandsExecutorProperties;

   private Properties evictionScheduledExecutorProperties;

   private Properties replicationQueueScheduledExecutorProperties;

   private Short marshallVersion;

   public void setExposeGlobalJmxStatistics(Boolean exposeGlobalJmxStatistics) {
      this.exposeGlobalJmxStatistics = exposeGlobalJmxStatistics;
   }

   public void setmBeanServerProperties(Properties mBeanServerProperties) {
      this.mBeanServerProperties = mBeanServerProperties;
   }

   public void setJmxDomain(String jmxDomain) {
      this.jmxDomain = jmxDomain;
   }

   public void setmBeanServerLookupClass(String mBeanServerLookupClass) {
      this.mBeanServerLookupClass = mBeanServerLookupClass;
   }

   public void setmBeanServerLookup(MBeanServerLookup mBeanServerLookup) {
      this.mBeanServerLookup = mBeanServerLookup;
   }

   public void setAllowDuplicateDomains(Boolean allowDuplicateDomains) {
      this.allowDuplicateDomains = allowDuplicateDomains;
   }

   public void setCacheManagerName(String cacheManagerName) {
      this.cacheManagerName = cacheManagerName;
   }

   public void setClusterName(String clusterName) {
      this.clusterName = clusterName;
   }

   public void setMachineId(String machineId) {
      this.machineId = machineId;
   }

   public void setRackId(String rackId) {
      this.rackId = rackId;
   }

   public void setSiteId(String siteId) {
      this.siteId = siteId;
   }

   public void setStrictPeerToPeer(Boolean strictPeerToPeer) {
      this.strictPeerToPeer = strictPeerToPeer;
   }

   public void setDistributedSyncTimeout(Long distributedSyncTimeout) {
      this.distributedSyncTimeout = distributedSyncTimeout;
   }

   public void setTransportClass(String transportClass) {
      this.transportClass = transportClass;
   }

   public void setTransportNodeName(String transportNodeName) {
      this.transportNodeName = transportNodeName;
   }

   public void setAsyncListenerExecutorFactoryClass(String asyncListenerExecutorFactoryClass) {
      this.asyncListenerExecutorFactoryClass = asyncListenerExecutorFactoryClass;
   }

   public void setAsyncTransportExecutorFactoryClass(String asyncTransportExecutorFactoryClass) {
      this.asyncTransportExecutorFactoryClass = asyncTransportExecutorFactoryClass;
   }

   public void setRemoteCommandsExecutorFactoryClass(String remoteCommandsExecutorFactoryClass) {
      this.remoteCommandsExecutorFactoryClass = remoteCommandsExecutorFactoryClass;
   }

   public void setEvictionScheduledExecutorFactoryClass(String evictionScheduledExecutorFactoryClass) {
      this.evictionScheduledExecutorFactoryClass = evictionScheduledExecutorFactoryClass;
   }

   public void setReplicationQueueScheduledExecutorFactoryClass(String replicationQueueScheduledExecutorFactoryClass) {
      this.replicationQueueScheduledExecutorFactoryClass = replicationQueueScheduledExecutorFactoryClass;
   }

   public void setMarshallerClass(String marshallerClass) {
      this.marshallerClass = marshallerClass;
   }

   public void setTransportProperties(Properties transportProperties) {
      this.transportProperties = transportProperties;
   }

   public void setShutdownHookBehavior(String shutdownHookBehavior) {
      this.shutdownHookBehavior = shutdownHookBehavior;
   }

   public void setAsyncTransportExecutorProperties(Properties asyncTransportExecutorProperties) {
      this.asyncTransportExecutorProperties = asyncTransportExecutorProperties;
   }

   public void setAsyncListenerExecutorProperties(Properties asyncListenerExecutorProperties) {
      this.asyncListenerExecutorProperties = asyncListenerExecutorProperties;
   }

   public void setRemoteCommandsExecutorProperties(Properties remoteCommandsExecutorProperties) {
      this.remoteCommandsExecutorProperties = remoteCommandsExecutorProperties;
   }

   public void setEvictionScheduledExecutorProperties(Properties evictionScheduledExecutorProperties) {
      this.evictionScheduledExecutorProperties = evictionScheduledExecutorProperties;
   }

   public void setReplicationQueueScheduledExecutorProperties(Properties replicationQueueScheduledExecutorProperties) {
      this.replicationQueueScheduledExecutorProperties = replicationQueueScheduledExecutorProperties;
   }

   public void setMarshallVersion(Short marshallVersion) {
      this.marshallVersion = marshallVersion;
   }

   public void applyOverridesTo(final GlobalConfigurationBuilder globalConfigurationToOverride) {
      this.logger.debug("Applying configuration overrides to GlobalConfiguration ["
            + globalConfigurationToOverride + "] ...");

      if (this.exposeGlobalJmxStatistics != null) {
         this.logger.debug("Overriding property [exposeGlobalJmxStatistics] with new value ["
               + this.exposeGlobalJmxStatistics + "]");
         globalConfigurationToOverride.globalJmxStatistics().enabled(this.exposeGlobalJmxStatistics);
      }
      if (this.mBeanServerProperties != null) {
         this.logger.debug("Overriding property [mBeanServerProperties] with new value ["
               + this.mBeanServerProperties + "]");
         globalConfigurationToOverride.globalJmxStatistics().withProperties(this.mBeanServerProperties);
      }
      if (this.jmxDomain != null) {
         this.logger.debug("Overriding property [jmxDomain] with new value [" + this.jmxDomain
               + "]");
         globalConfigurationToOverride.globalJmxStatistics().jmxDomain(this.jmxDomain);
      }
      if (this.mBeanServerLookupClass != null) {
         this.logger.debug("Overriding property [mBeanServerLookupClass] with new value ["
               + this.mBeanServerLookupClass + "]");
         globalConfigurationToOverride.globalJmxStatistics().mBeanServerLookup(
               Util.<MBeanServerLookup>getInstance(this.mBeanServerLookupClass,
                     Thread.currentThread().getContextClassLoader()));
      }
      if (this.mBeanServerLookup != null) {
         this.logger.debug("Overriding property [mBeanServerLookup] with new value ["
               + this.mBeanServerLookup + "]");
         globalConfigurationToOverride.globalJmxStatistics().mBeanServerLookup(this.mBeanServerLookup);
      }
      if (this.allowDuplicateDomains != null) {
         this.logger.debug("Overriding property [allowDuplicateDomains] with new value ["
               + this.allowDuplicateDomains + "]");
         globalConfigurationToOverride.globalJmxStatistics().allowDuplicateDomains(this.allowDuplicateDomains);
      }
      if (this.cacheManagerName != null) {
         this.logger.debug("Overriding property [cacheManagerName] with new value ["
               + this.cacheManagerName + "]");
         globalConfigurationToOverride.globalJmxStatistics().cacheManagerName(this.cacheManagerName);
      }
      if (this.clusterName != null) {
         this.logger.debug("Overriding property [clusterName] with new value ["
               + this.clusterName + "]");
         globalConfigurationToOverride.transport().clusterName(this.clusterName);
      }
      if (this.machineId != null) {
         this.logger.debug("Overriding property [machineId] with new value [" + this.machineId
               + "]");
         globalConfigurationToOverride.transport().machineId(this.machineId);
      }
      if (this.rackId != null) {
         this.logger.debug("Overriding property [rackId] with new value [" + this.rackId + "]");
         globalConfigurationToOverride.transport().rackId(this.rackId);
      }
      if (this.siteId != null) {
         this.logger.debug("Overriding property [siteId] with new value [" + this.siteId + "]");
         globalConfigurationToOverride.transport().siteId(this.siteId);
      }
      if (this.strictPeerToPeer != null) {
         this.logger.debug("Overriding property [strictPeerToPeer] with new value ["
               + this.strictPeerToPeer + "]");
         globalConfigurationToOverride.transport().strictPeerToPeer(this.strictPeerToPeer);
      }
      if (this.distributedSyncTimeout != null) {
         this.logger.debug("Overriding property [distributedSyncTimeout] with new value ["
               + this.distributedSyncTimeout + "]");
         globalConfigurationToOverride.transport().distributedSyncTimeout(this.distributedSyncTimeout);
      }
      if (this.transportClass != null) {
         this.logger.debug("Overriding property [transportClass] with new value ["
               + this.transportClass + "]");
         globalConfigurationToOverride.transport().transport(
               Util.<Transport>getInstance(this.transportClass,
                     Thread.currentThread().getContextClassLoader()));
      }
      if (this.transportNodeName != null) {
         this.logger.debug("Overriding property [transportNodeName] with new value ["
               + this.transportNodeName + "]");
         globalConfigurationToOverride.transport().nodeName(this.transportNodeName);
      }
      if (this.asyncListenerExecutorFactoryClass != null) {
         this.logger
               .debug("Overriding property [asyncListenerExecutorFactoryClass] with new value ["
                     + this.asyncListenerExecutorFactoryClass + "]");
         globalConfigurationToOverride.asyncListenerExecutor().factory(
               Util.<ExecutorFactory>getInstance(this.asyncListenerExecutorFactoryClass,
                     Thread.currentThread().getContextClassLoader()));
      }
      if (this.asyncTransportExecutorFactoryClass != null) {
         this.logger
               .debug("Overriding property [asyncTransportExecutorFactoryClass] with new value ["
                     + this.asyncTransportExecutorFactoryClass + "]");
         globalConfigurationToOverride.asyncTransportExecutor().factory(
               Util.<ExecutorFactory>getInstance(this.asyncTransportExecutorFactoryClass,
                     Thread.currentThread().getContextClassLoader()));
      }
      if (this.remoteCommandsExecutorFactoryClass != null) {
         this.logger
               .debug("Overriding property [remoteCommandsExecutorFactoryClass] with new value ["
                     + this.remoteCommandsExecutorFactoryClass + "]");
         globalConfigurationToOverride.remoteCommandsExecutor().factory(
               Util.<ExecutorFactory>getInstance(this.remoteCommandsExecutorFactoryClass,
                     Thread.currentThread().getContextClassLoader()));
      }
      if (this.evictionScheduledExecutorFactoryClass != null) {
         this.logger
               .debug("Overriding property [evictionScheduledExecutorFactoryClass] with new value ["
                     + this.evictionScheduledExecutorFactoryClass + "]");
         globalConfigurationToOverride.evictionScheduledExecutor().factory(
               Util.<ScheduledExecutorFactory>getInstance(this.evictionScheduledExecutorFactoryClass,
                     Thread.currentThread().getContextClassLoader()));
      }
      if (this.replicationQueueScheduledExecutorFactoryClass != null) {
         this.logger
               .debug("Overriding property [replicationQueueScheduledExecutorFactoryClass] with new value ["
                     + this.replicationQueueScheduledExecutorFactoryClass + "]");
         globalConfigurationToOverride.replicationQueueScheduledExecutor().factory(
               Util.<ScheduledExecutorFactory>getInstance(this.replicationQueueScheduledExecutorFactoryClass,
                     Thread.currentThread().getContextClassLoader()));
      }
      if (this.marshallerClass != null) {
         this.logger.debug("Overriding property [marshallerClass] with new value ["
               + this.marshallerClass + "]");
         globalConfigurationToOverride.serialization().marshaller(
               Util.<Marshaller>getInstance(this.marshallerClass,
                     Thread.currentThread().getContextClassLoader()));
      }
      if (this.transportProperties != null) {
         this.logger.debug("Overriding property [transportProperties] with new value ["
               + this.transportProperties + "]");
         globalConfigurationToOverride.transport().withProperties(this.transportProperties);
      }
      if (this.shutdownHookBehavior != null) {
         this.logger.debug("Overriding property [shutdownHookBehavior] with new value ["
               + this.shutdownHookBehavior + "]");
         globalConfigurationToOverride.shutdown().hookBehavior(
               ShutdownHookBehavior.valueOf(this.shutdownHookBehavior));
      }
      if (this.asyncListenerExecutorProperties != null) {
         this.logger
               .debug("Overriding property [asyncListenerExecutorProperties] with new value ["
                     + this.asyncListenerExecutorProperties + "]");
         globalConfigurationToOverride.asyncListenerExecutor().withProperties(
               this.asyncListenerExecutorProperties);
      }
      if (this.asyncTransportExecutorProperties != null) {
         this.logger
               .debug("Overriding property [asyncTransportExecutorProperties] with new value ["
                     + this.asyncTransportExecutorProperties + "]");
         globalConfigurationToOverride.asyncTransportExecutor().withProperties(
               this.asyncTransportExecutorProperties);
      }
      if (this.remoteCommandsExecutorProperties != null) {
         this.logger
               .debug("Overriding property [remoteCommandsExecutorProperties] with new value ["
                     + this.remoteCommandsExecutorProperties + "]");
         globalConfigurationToOverride.remoteCommandsExecutor().withProperties(
               this.remoteCommandsExecutorProperties);
      }
      if (this.evictionScheduledExecutorProperties != null) {
         this.logger
               .debug("Overriding property [evictionScheduledExecutorProperties] with new value ["
                     + this.evictionScheduledExecutorProperties + "]");
         globalConfigurationToOverride.evictionScheduledExecutor()
               .withProperties(this.evictionScheduledExecutorProperties);
      }
      if (this.replicationQueueScheduledExecutorProperties != null) {
         this.logger
               .debug("Overriding property [replicationQueueScheduledExecutorProperties] with new value ["
                     + this.replicationQueueScheduledExecutorProperties + "]");
         globalConfigurationToOverride.replicationQueueScheduledExecutor()
               .withProperties(this.replicationQueueScheduledExecutorProperties);
      }
      if (this.marshallVersion != null) {
         this.logger.debug("Overriding property [marshallVersion] with new value ["
               + this.marshallVersion + "]");
         globalConfigurationToOverride.serialization().version(this.marshallVersion);
      }

      this.logger.debug("Finished applying configuration overrides to GlobalConfiguration ["
            + globalConfigurationToOverride + "]");
   }

}
