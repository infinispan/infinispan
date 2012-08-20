/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.global;

import org.infinispan.config.ConfigurationException;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.Util;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Configures the transport used for network communications across the cluster.
 */
public class TransportConfigurationBuilder extends AbstractGlobalConfigurationBuilder<TransportConfiguration> {

   // Lazily instantiate this if the user doesn't request an alternate to avoid a hard dep on jgroups library
   public static final Class<? extends Transport> DEFAULT_TRANSPORT = JGroupsTransport.class;

   private String clusterName = "ISPN";
   private String machineId;
   private String rackId;
   private String siteId;
   private long distributedSyncTimeout = TimeUnit.MINUTES.toMillis(4);
   private Transport transport;

   private String nodeName;
   private Properties properties = new Properties();
   private boolean strictPeerToPeer = false;

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
   void validate() {
      if(clusterName == null){
          throw new ConfigurationException("Transport clusterName cannot be null");
      }
   }

   @Override
   TransportConfiguration create() {
      return new TransportConfiguration(clusterName, machineId, rackId, siteId, strictPeerToPeer, distributedSyncTimeout, transport, nodeName, TypedProperties.toTypedProperties(properties));
   }

   public TransportConfigurationBuilder defaultTransport() {
      transport(Util.getInstance(DEFAULT_TRANSPORT));
      return this;
   }

   @Override
   protected
   TransportConfigurationBuilder read(TransportConfiguration template) {
      this.clusterName = template.clusterName();
      this.distributedSyncTimeout = template.distributedSyncTimeout();
      this.machineId = template.machineId();
      this.nodeName = template.nodeName();
      this.properties = template.properties();
      this.rackId = template.rackId();
      this.siteId = template.siteId();
      this.strictPeerToPeer = template.strictPeerToPeer();
      this.transport = template.transport();

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
            ", strictPeerToPeer=" + strictPeerToPeer +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TransportConfigurationBuilder that = (TransportConfigurationBuilder) o;

      if (distributedSyncTimeout != that.distributedSyncTimeout) return false;
      if (strictPeerToPeer != that.strictPeerToPeer) return false;
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
      result = 31 * result + (strictPeerToPeer ? 1 : 0);
      return result;
   }

}
