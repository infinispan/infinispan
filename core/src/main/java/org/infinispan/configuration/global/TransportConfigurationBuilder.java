package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

import static org.infinispan.configuration.global.TransportConfiguration.*;

/**
 * Configures the transport used for network communications across the cluster.
 */
public class TransportConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<TransportConfiguration> {
   private static final Log log = LogFactory.getLog(TransportConfigurationBuilder.class);

   // Lazily instantiate this if the user doesn't request an alternate to avoid a hard dep on jgroups library
   public static final String DEFAULT_TRANSPORT = "org.infinispan.remoting.transport.jgroups.JGroupsTransport";

   private final ThreadPoolConfigurationBuilder transportThreadPool;
   private final ThreadPoolConfigurationBuilder remoteCommandThreadPool;
   @Deprecated
   private final ThreadPoolConfigurationBuilder totalOrderThreadPool;
   private final AttributeSet attributes;

   TransportConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = TransportConfiguration.attributeSet();
      transportThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      remoteCommandThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
      totalOrderThreadPool = new ThreadPoolConfigurationBuilder(globalConfig);
   }

   /**
    * Defines the name of the cluster. Nodes only connect to clusters sharing the same name.
    *
    * @param clusterName
    */
   public TransportConfigurationBuilder clusterName(String clusterName) {
      attributes.attribute(CLUSTER_NAME).set(clusterName);
      return this;
   }

   /**
    * The id of the machine where this node runs. Used for <a
    * href="http://community.jboss.org/wiki/DesigningServerHinting">server hinting</a> .
    */
   public TransportConfigurationBuilder machineId(String machineId) {
      attributes.attribute(MACHINE_ID).set(machineId);
      return this;
   }

   /**
    * The id of the rack where this node runs. Used for <a
    * href="http://community.jboss.org/wiki/DesigningServerHinting">server hinting</a> .
    */
   public TransportConfigurationBuilder rackId(String rackId) {
      attributes.attribute(RACK_ID).set(rackId);
      return this;
   }

   /**
    * The id of the site where this node runs. Used for <a
    * href="http://community.jboss.org/wiki/DesigningServerHinting">server hinting</a> .
    */
   public TransportConfigurationBuilder siteId(String siteId) {
      attributes.attribute(SITE_ID).set(siteId);
      return this;
   }

   /**
    * Timeout for coordinating cluster formation when nodes join or leave the cluster.
    *
    * @param distributedSyncTimeout
    * @return
    */
   public TransportConfigurationBuilder distributedSyncTimeout(long distributedSyncTimeout) {
      attributes.attribute(DISTRIBUTED_SYNC_TIMEOUT).set(distributedSyncTimeout);
      return this;
   }

   /**
    * Timeout for coordinating cluster formation when nodes join or leave the cluster.
    *
    * @param distributedSyncTimeout
    * @return
    */
   public TransportConfigurationBuilder distributedSyncTimeout(long distributedSyncTimeout, TimeUnit unit) {
      return distributedSyncTimeout(unit.toMillis(distributedSyncTimeout));
   }

   /**
    * Class that represents a network transport. Must implement
    * org.infinispan.remoting.transport.Transport
    *
    * NOTE: Currently Infinispan will not use the object instance, but instead instantiate a new
    * instance of the class. Therefore, do not expect any state to survive, and provide a no-args
    * constructor to any instance. This will be resolved in Infinispan 5.2.0
    *
    * @param transport transport instance
    */
   public TransportConfigurationBuilder transport(Transport transport) {
      attributes.attribute(TRANSPORT).set(transport);
      return this;
   }

   /**
    * Name of the current node. This is a friendly name to make logs, etc. make more sense.
    * Defaults to a combination of host name and a random number (to differentiate multiple nodes
    * on the same host)
    *
    * @param nodeName
    */
   public TransportConfigurationBuilder nodeName(String nodeName) {
      attributes.attribute(NODE_NAME).set(nodeName);
      return this;
   }

   /**
    * Sets transport properties
    *
    * @param properties
    * @return this TransportConfig
    */
   public TransportConfigurationBuilder withProperties(Properties properties) {
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(properties));
      return this;
   }

   /**
    * Clears the transport properties
    *
    * @return this TransportConfig
    */
   public TransportConfigurationBuilder clearProperties() {
      attributes.attribute(PROPERTIES).set(new TypedProperties());
      return this;
   }

   public TransportConfigurationBuilder addProperty(String key, String value) {
      TypedProperties properties = attributes.attribute(PROPERTIES).get();
      properties.put(key, value);
      attributes.attribute(PROPERTIES).set(properties);
      return this;
   }

   public TransportConfigurationBuilder removeProperty(String key) {
      TypedProperties properties = attributes.attribute(PROPERTIES).get();
      properties.remove(key);
      attributes.attribute(PROPERTIES).set(properties);
      return this;
   }

   public String getProperty(String key) {
      return String.valueOf(attributes.attribute(PROPERTIES).get().get(key));
   }

   /**
    * @deprecated Since 6.0, strictPeerToPeer is ignored and asymmetric clusters are always allowed.
    */
   @Deprecated
   public TransportConfigurationBuilder strictPeerToPeer(Boolean ignored) {
      log.strictPeerToPeerDeprecated();
      return this;
   }

   public ThreadPoolConfigurationBuilder transportThreadPool() {
      return transportThreadPool;
   }

   public ThreadPoolConfigurationBuilder remoteCommandThreadPool() {
      return remoteCommandThreadPool;
   }

   @Deprecated
   public ThreadPoolConfigurationBuilder totalOrderThreadPool() {
      return totalOrderThreadPool;
   }

   @Override
   public
   void validate() {
      for (Builder<?> validatable : asList(transportThreadPool,
            remoteCommandThreadPool, totalOrderThreadPool)) {
         validatable.validate();
      }
      if(attributes.attribute(CLUSTER_NAME).get() == null){
          throw new CacheConfigurationException("Transport clusterName cannot be null");
      }
   }

   @Override
   public
   TransportConfiguration create() {
      return new TransportConfiguration(attributes.protect(), transportThreadPool.create(), remoteCommandThreadPool.create(), totalOrderThreadPool.create());
   }

   public TransportConfigurationBuilder defaultTransport() {
      Transport transport = Util.getInstance(DEFAULT_TRANSPORT, this.getGlobalConfig().getClassLoader());
      transport(transport);
      return this;
   }

   @Override
   public
   TransportConfigurationBuilder read(TransportConfiguration template) {
      attributes.read(template.attributes());
      this.remoteCommandThreadPool.read(template.remoteCommandThreadPool());
      this.totalOrderThreadPool.read(template.totalOrderThreadPool());
      this.transportThreadPool.read(template.transportThreadPool());
      if (template.transport() != null) {
         Transport transport = Util.getInstance(template.transport().getClass().getName(), template.transport().getClass().getClassLoader());
         transport(transport);
      }

      return this;
   }

   public Transport getTransport() {
      return attributes.attribute(TRANSPORT).get();
   }

   @Override
   public String toString() {
      return "TransportConfigurationBuilder [transportThreadPool=" + transportThreadPool + ", remoteCommandThreadPool="
            + remoteCommandThreadPool + ", totalOrderThreadPool=" + totalOrderThreadPool + ", attributes=" + attributes
            + "]";
   }

}
