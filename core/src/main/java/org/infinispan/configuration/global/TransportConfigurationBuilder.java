package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.TransportConfiguration.CLUSTER_NAME;
import static org.infinispan.configuration.global.TransportConfiguration.DISTRIBUTED_SYNC_TIMEOUT;
import static org.infinispan.configuration.global.TransportConfiguration.INITIAL_CLUSTER_SIZE;
import static org.infinispan.configuration.global.TransportConfiguration.INITIAL_CLUSTER_TIMEOUT;
import static org.infinispan.configuration.global.TransportConfiguration.MACHINE_ID;
import static org.infinispan.configuration.global.TransportConfiguration.NODE_NAME;
import static org.infinispan.configuration.global.TransportConfiguration.RACK_ID;
import static org.infinispan.configuration.global.TransportConfiguration.REMOTE_EXECUTOR;
import static org.infinispan.configuration.global.TransportConfiguration.SITE_ID;
import static org.infinispan.configuration.global.TransportConfiguration.STACK;
import static org.infinispan.configuration.global.TransportConfiguration.TRANSPORT_EXECUTOR;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Transport;

/**
 * Configures the transport used for network communications across the cluster.
 */
public class TransportConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<TransportConfiguration> {

   // Lazily instantiate this if the user doesn't request an alternate to avoid a hard dep on jgroups library
   public static final String DEFAULT_TRANSPORT = "org.infinispan.remoting.transport.jgroups.JGroupsTransport";

   private final ThreadsConfigurationBuilder threads;
   private final AttributeSet attributes;
   private final JGroupsConfigurationBuilder jgroupsConfigurationBuilder;
   private TypedProperties typedProperties = new TypedProperties();

   TransportConfigurationBuilder(GlobalConfigurationBuilder globalConfig, ThreadsConfigurationBuilder threads) {
      super(globalConfig);
      this.threads = threads;
      this.attributes = TransportConfiguration.attributeSet();
      this.jgroupsConfigurationBuilder = new JGroupsConfigurationBuilder(globalConfig);
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

   public TransportConfigurationBuilder stack(String stack) {
      attributes.attribute(STACK).set(stack);
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
    * Sets the number of nodes that need to join before the cache container can start. The default is to start
    * immediately without waiting.
    */
   public TransportConfigurationBuilder initialClusterSize(int clusterSize) {
      attributes.attribute(INITIAL_CLUSTER_SIZE).set(clusterSize);
      return this;
   }

   /**
    * Sets the timeout for the initial cluster to form. Defaults to 1 minute
    */
   public TransportConfigurationBuilder initialClusterTimeout(long initialClusterTimeout, TimeUnit unit) {
      attributes.attribute(INITIAL_CLUSTER_TIMEOUT).set(unit.toMillis(initialClusterTimeout));
      return this;
   }

   /**
    * Class that represents a network transport. Must implement
    * org.infinispan.remoting.transport.Transport
    *
    * NOTE: Currently Infinispan will not use the object instance, but instead instantiate a new
    * instance of the class. Therefore, do not expect any state to survive, and provide a no-args
    * constructor to any instance.
    *
    * @param transport transport instance
    */
   public TransportConfigurationBuilder transport(Transport transport) {
      jgroupsConfigurationBuilder.transport(transport);
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
      this.typedProperties = TypedProperties.toTypedProperties(properties);
      return this;
   }

   /**
    * Clears the transport properties
    *
    * @return this TransportConfig
    */
   public TransportConfigurationBuilder clearProperties() {
      typedProperties.clear();
      return this;
   }

   public TransportConfigurationBuilder addProperty(String key, Object value) {
      typedProperties.put(key, value);
      return this;
   }

   public TransportConfigurationBuilder removeProperty(String key) {
      typedProperties.remove(key);
      return this;
   }

   public String getProperty(String key) {
      return String.valueOf(typedProperties.get(key));
   }

   public ThreadPoolConfigurationBuilder transportThreadPool() {
      return threads.transportThreadPool();
   }

   public ThreadPoolConfigurationBuilder remoteCommandThreadPool() {
      return threads.remoteCommandThreadPool();
   }

   @Override
   public
   void validate() {
      if(attributes.attribute(CLUSTER_NAME).get() == null){
          throw new CacheConfigurationException("Transport clusterName cannot be null");
      }
   }

   public JGroupsConfigurationBuilder jgroups() {
      return jgroupsConfigurationBuilder;
   }

   @Override
   public
   TransportConfiguration create() {
      //if (typedProperties.containsKey("stack")) attributes.attribute(STACK).set(typedProperties.getProperty("stack"));
      return new TransportConfiguration(attributes.protect(),
            jgroupsConfigurationBuilder.create(), threads.transportThreadPool().create(), threads.remoteCommandThreadPool().create(), typedProperties);
   }

   public TransportConfigurationBuilder defaultTransport() {
      Transport transport = Util.getInstance(DEFAULT_TRANSPORT, this.getGlobalConfig().getClassLoader());
      transport(transport);
      return this;
   }

   @Deprecated
   public TransportConfigurationBuilder transportExecutor(String threadPoolName) {
      attributes.attribute(TRANSPORT_EXECUTOR).set(threadPoolName);
      return this;
   }

   public TransportConfigurationBuilder remoteExecutor(String threadPoolName) {
      attributes.attribute(REMOTE_EXECUTOR).set(threadPoolName);
      return this;
   }

   @Override
   public
   TransportConfigurationBuilder read(TransportConfiguration template) {
      attributes.read(template.attributes());
      this.jgroupsConfigurationBuilder.read(template.jgroups());
      this.typedProperties = new TypedProperties(template.properties());
      if (template.transport() != null) {
         Transport transport = Util.getInstance(template.transport().getClass().getName(), template.transport().getClass().getClassLoader());
         transport(transport);
      }

      return this;
   }

   public Transport getTransport() {
      return jgroupsConfigurationBuilder.jgroupsTransport();
   }

   @Override
   public String toString() {
      return "TransportConfigurationBuilder{" +
            "threads=" + threads +
            ", attributes=" + attributes +
            ", jgroupsConfigurationBuilder=" + jgroupsConfigurationBuilder +
            ", typedProperties=" + typedProperties +
            '}';
   }
}
