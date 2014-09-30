package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

/**
 * Configures the transport used for network communications across the cluster.
 */
public class TransportConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<TransportConfiguration> {
   private static final Log log = LogFactory.getLog(TransportConfigurationBuilder.class);

   // Lazily instantiate this if the user doesn't request an alternate to avoid a hard dep on jgroups library
   public static final String DEFAULT_TRANSPORT = "org.infinispan.remoting.transport.jgroups.JGroupsTransport";

   private String clusterName = "ISPN";
   private String machineId;
   private String rackId;
   private String siteId;
   private long distributedSyncTimeout = TimeUnit.MINUTES.toMillis(4);
   private Transport transport;

   private String nodeName;
   private Properties properties = new Properties();
   private final ThreadPoolConfigurationBuilder transportThreadPool;
   private final ThreadPoolConfigurationBuilder remoteCommandThreadPool;
   private final ThreadPoolConfigurationBuilder totalOrderThreadPool;

   TransportConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
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
    * Timeout for coordinating cluster formation when nodes join or leave the cluster.
    *
    * @param distributedSyncTimeout
    * @return
    */
   public TransportConfigurationBuilder distributedSyncTimeout(long distributedSyncTimeout) {
      this.distributedSyncTimeout = distributedSyncTimeout;
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
    * @return this TransportConfig
    */
   public TransportConfigurationBuilder clearProperties() {
      this.properties = new Properties();
      return this;
   }

   public TransportConfigurationBuilder addProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
   }

   public TransportConfigurationBuilder removeProperty(String key) {
      this.properties.remove(key);
      return this;
   }

   public String getProperty(String key) {
      return String.valueOf(this.properties.get(key));
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
      if(clusterName == null){
          throw new CacheConfigurationException("Transport clusterName cannot be null");
      }
   }

   @Override
   public
   TransportConfiguration create() {
      return new TransportConfiguration(clusterName, machineId, rackId, siteId,
            distributedSyncTimeout, transport, nodeName, TypedProperties.toTypedProperties(properties),
            transportThreadPool.create(), remoteCommandThreadPool.create(), totalOrderThreadPool.create());
   }

   public TransportConfigurationBuilder defaultTransport() {
      Transport transport = Util.getInstance(DEFAULT_TRANSPORT, this.getGlobalConfig().getClassLoader());
      transport(transport);
      return this;
   }

   @Override
   public
   TransportConfigurationBuilder read(TransportConfiguration template) {
      this.clusterName = template.clusterName();
      this.distributedSyncTimeout = template.distributedSyncTimeout();
      this.machineId = template.machineId();
      this.nodeName = template.nodeName();
      this.properties = template.properties();
      this.rackId = template.rackId();
      this.siteId = template.siteId();
      this.remoteCommandThreadPool.read(template.remoteCommandThreadPool());
      this.totalOrderThreadPool.read(template.totalOrderThreadPool());
      this.transportThreadPool.read(template.transportThreadPool());
      if (template.transport() != null) {
         this.transport = Util.getInstance(template.transport().getClass().getName(), template.transport().getClass().getClassLoader());
      }

      return this;
   }

   public Transport getTransport() {
      return transport;
   }

   @Override
   public String toString() {
      return "TransportConfigurationBuilder{" +
            "clusterName='" + clusterName + '\'' +
            ", machineId='" + machineId + '\'' +
            ", rackId='" + rackId + '\'' +
            ", siteId='" + siteId + '\'' +
            ", distributedSyncTimeout=" + distributedSyncTimeout +
            ", transport=" + transport +
            ", nodeName='" + nodeName + '\'' +
            ", properties=" + properties +
            ", threadPool=" + transportThreadPool +
            ", remoteCommandThreadPool=" + remoteCommandThreadPool +
            ", totalOrderThreadPool=" + totalOrderThreadPool +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TransportConfigurationBuilder that = (TransportConfigurationBuilder) o;

      if (distributedSyncTimeout != that.distributedSyncTimeout) return false;
      if (clusterName != null ? !clusterName.equals(that.clusterName) : that.clusterName != null)
         return false;
      if (machineId != null ? !machineId.equals(that.machineId) : that.machineId != null)
         return false;
      if (nodeName != null ? !nodeName.equals(that.nodeName) : that.nodeName != null)
         return false;
      if (properties != null ? !properties.equals(that.properties) : that.properties != null)
         return false;
      if (rackId != null ? !rackId.equals(that.rackId) : that.rackId != null)
         return false;
      if (siteId != null ? !siteId.equals(that.siteId) : that.siteId != null)
         return false;
      if (transport != null ? !transport.equals(that.transport) : that.transport != null)
         return false;
      if (!transportThreadPool.equals(that.transportThreadPool))
         return false;
      if (!remoteCommandThreadPool.equals(that.remoteCommandThreadPool))
         return false;
      if (!totalOrderThreadPool.equals(that.totalOrderThreadPool))
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = clusterName != null ? clusterName.hashCode() : 0;
      result = 31 * result + (machineId != null ? machineId.hashCode() : 0);
      result = 31 * result + (rackId != null ? rackId.hashCode() : 0);
      result = 31 * result + (siteId != null ? siteId.hashCode() : 0);
      result = 31 * result + (int) (distributedSyncTimeout ^ (distributedSyncTimeout >>> 32));
      result = 31 * result + (transport != null ? transport.hashCode() : 0);
      result = 31 * result + (nodeName != null ? nodeName.hashCode() : 0);
      result = 31 * result + (properties != null ? properties.hashCode() : 0);
      result = 31 * result + (transportThreadPool.hashCode());
      result = 31 * result + (remoteCommandThreadPool.hashCode());
      result = 31 * result + (totalOrderThreadPool.hashCode());
      return result;
   }

}
