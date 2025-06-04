package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.TransportConfiguration.CLUSTER_NAME;
import static org.infinispan.configuration.global.TransportConfiguration.DISTRIBUTED_SYNC_TIMEOUT;
import static org.infinispan.configuration.global.TransportConfiguration.INITIAL_CLUSTER_SIZE;
import static org.infinispan.configuration.global.TransportConfiguration.INITIAL_CLUSTER_TIMEOUT;
import static org.infinispan.configuration.global.TransportConfiguration.MACHINE_ID;
import static org.infinispan.configuration.global.TransportConfiguration.NODE_NAME;
import static org.infinispan.configuration.global.TransportConfiguration.RACK_ID;
import static org.infinispan.configuration.global.TransportConfiguration.RAFT_MEMBERS;
import static org.infinispan.configuration.global.TransportConfiguration.SITE_ID;
import static org.infinispan.configuration.global.TransportConfiguration.STACK;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;

/**
 * Configures the transport used for network communications across the cluster.
 */
public class TransportConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<TransportConfiguration> {

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

   @Override
   public AttributeSet attributes() {
      return attributes;
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
      attributes.attribute(DISTRIBUTED_SYNC_TIMEOUT).set(TimeQuantity.valueOf(distributedSyncTimeout));
      return this;
   }

   /**
    * Same as {@link #distributedSyncTimeout(long)} but supporting time units
    */
   public TransportConfigurationBuilder distributedSyncTimeout(String  distributedSyncTimeout) {
      attributes.attribute(DISTRIBUTED_SYNC_TIMEOUT).set(TimeQuantity.valueOf(distributedSyncTimeout));
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
      attributes.attribute(INITIAL_CLUSTER_TIMEOUT).set(TimeQuantity.valueOf(unit.toMillis(initialClusterTimeout)));
      return this;
   }

   /**
    * Same as {@link #initialClusterTimeout(long, TimeUnit)} but supporting time units.
    */

   public TransportConfigurationBuilder initialClusterTimeout(String initialClusterTimeout) {
      attributes.attribute(INITIAL_CLUSTER_TIMEOUT).set(TimeQuantity.valueOf(initialClusterTimeout));
      return this;
   }

   /**
    * Class that represents a network transport. Must implement
    * org.infinispan.remoting.transport.Transport
    *
    * @param transport transport instance
    */
   public TransportConfigurationBuilder transport(JGroupsTransport transport) {
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

   @Override
   public
   void validate() {
      if(attributes.attribute(CLUSTER_NAME).isNull()){
         throw CONFIG.requireNonNullClusterName();
      }
      validateRaftMembers();
   }

   public JGroupsConfigurationBuilder jgroups() {
      return jgroupsConfigurationBuilder;
   }

   @Override
   public
   TransportConfiguration create() {
      //if (typedProperties.containsKey("stack")) attributes.attribute(STACK).set(typedProperties.getProperty("stack"));
      return new TransportConfiguration(attributes.protect(), jgroupsConfigurationBuilder.create(), typedProperties);
   }

   public TransportConfigurationBuilder defaultTransport() {
      transport(new JGroupsTransport());
      return this;
   }

   /**
    * Adds a single member to the {@code raft-members}.
    *
    * @param member The member to add
    */
   public TransportConfigurationBuilder raftMember(String member) {
      Set<String> newMembers = new HashSet<>(attributes.attribute(RAFT_MEMBERS).get());
      if (newMembers.add(member)) {
         attributes.attribute(RAFT_MEMBERS).set(Collections.unmodifiableSet(newMembers));
      }
      return this;
   }

   /**
    * Adds multiple members to the {@code raft-members}.
    *
    * @param members The members to add
    */
   public TransportConfigurationBuilder raftMembers(String... members) {
      return raftMembers(Arrays.asList(members));
   }

   /**
    * Adds multiple members to the {@code raft-members}.
    *
    * @param members The members to add
    */
   public TransportConfigurationBuilder raftMembers(Collection<String> members) {
      Set<String> newMembers = new HashSet<>(attributes.attribute(RAFT_MEMBERS).get());
      if (newMembers.addAll(members)) {
         attributes.attribute(RAFT_MEMBERS).set(Collections.unmodifiableSet(newMembers));
      }
      return this;
   }

   @Override
   public
   TransportConfigurationBuilder read(TransportConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      this.jgroupsConfigurationBuilder.read(template.jgroups(), combine);
      this.typedProperties = new TypedProperties(template.properties());
      if (template.transport() != null) {
         transport(new JGroupsTransport());
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

   private void validateRaftMembers() {
      Set<String> raftMembers = attributes.attribute(RAFT_MEMBERS).get();
      if (raftMembers.isEmpty()) {
         //RAFT not enabled
         return;
      }
      String raftId = attributes.attribute(NODE_NAME).get();
      if (raftId == null || raftId.isEmpty()) {
         throw CONFIG.requireNodeName();
      }
      if (!raftMembers.contains(raftId)) {
         throw CONFIG.nodeNameNotInRaftMembers(String.valueOf(raftMembers));
      }
   }
}
