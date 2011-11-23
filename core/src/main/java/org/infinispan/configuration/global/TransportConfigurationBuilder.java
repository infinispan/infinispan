package org.infinispan.configuration.global;

import java.util.Properties;

import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.TypedProperties;

/**
 * Configures the transport used for network communications across the cluster.
 */
public class TransportConfigurationBuilder extends AbstractGlobalConfigurationBuilder<TransportConfiguration> {
   
   // Lazily instantiate this if the user doesn't request an alternate to avoid a hard dep on jgroups library
   private static final Class<? extends Transport> DEFAULT_TRANSPORT = JGroupsTransport.class; 
   
   private String clusterName = "Infinispan-Cluster";
   private String machineId;
   private String rackId;
   private String siteId;
   private long distributedSyncTimeout = 240000L;
   private Transport transport;
   
   private String nodeName;
   private Properties properties = new Properties();
   private Boolean strictPeerToPeer = true;
   
   TransportConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
   }
   
   /**
    * Defines the name of the cluster. Nodes only connect to clusters sharing the same name.
    *
    * @param clusterName
    */
   public TransportConfigurationBuilder clusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
   }

   /**
    * The id of the machine where this node runs. Used for <a
    * href="http://community.jboss.org/wiki/DesigningServerHinting">server hinting</a> .
    */
   public TransportConfigurationBuilder machineId(String machineId) {
      this.machineId = machineId;
      return this;
   }

   /**
    * The id of the rack where this node runs. Used for <a
    * href="http://community.jboss.org/wiki/DesigningServerHinting">server hinting</a> .
    */
   public TransportConfigurationBuilder rackId(String rackId) {
      this.rackId = rackId;
      return this;
   }

   /**
    * The id of the site where this node runs. Used for <a
    * href="http://community.jboss.org/wiki/DesigningServerHinting">server hinting</a> .
    */
   public TransportConfigurationBuilder siteId(String siteId) {
      this.siteId = siteId;
      return this;
   }

   /**
    * TODO
    *
    * @param distributedSyncTimeout
    * @return
    */
   public TransportConfigurationBuilder distributedSyncTimeout(long distributedSyncTimeout) {
      this.distributedSyncTimeout = distributedSyncTimeout;
      return this;
   }

   /**
    * Class that represents a network transport. Must implement
    * org.infinispan.remoting.transport.Transport
    *
    * @param transportClass
    */
   public TransportConfigurationBuilder transport(Transport transport) {
      this.transport = transport;
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
      this.nodeName = nodeName;
      return this;
   }

   /**
    * Sets transport properties
    *
    * @param properties
    * @return this TransportConfig
    */
   public TransportConfigurationBuilder withProperties(Properties properties) {
      this.properties = properties;
      return this;
   }
   
   /**
    * Clears the transport properties
    *
    * @param properties
    * @return this TransportConfig
    */
   public TransportConfigurationBuilder clearProperties() {
      this.properties = new Properties();
      return this;
   }
   
   /**
    * TODO
    *
    * @param key
    * @param value
    * @return
    */
   public TransportConfigurationBuilder addProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
   }

   /**
    * If set to true, RPC operations will fail if the named cache does not exist on remote nodes
    * with a NamedCacheNotFoundException. Otherwise, operations will succeed but it will be
    * logged on the caller that the RPC did not succeed on certain nodes due to the named cache
    * not being available.
    *
    * @param strictPeerToPeer flag controlling this behavior
    */
   public TransportConfigurationBuilder strictPeerToPeer(Boolean strictPeerToPeer) {
      this.strictPeerToPeer = strictPeerToPeer;
      return this;
   }

   
   @Override
   void valididate() {
      // No-op, no validation required
   }
   
   @Override
   TransportConfiguration create() {
      Transport t = transport;
      if (t == null)
         defaultTransport();
      return new TransportConfiguration(clusterName, machineId, rackId, siteId, strictPeerToPeer, distributedSyncTimeout, t, nodeName, TypedProperties.toTypedProperties(properties));
   }
   
   public TransportConfigurationBuilder defaultTransport() {
      transport(Util.getInstance(DEFAULT_TRANSPORT));
      return this;
   }
   
   public TransportConfigurationBuilder clearTransport() {
      transport(null);
      return this;
   }
}