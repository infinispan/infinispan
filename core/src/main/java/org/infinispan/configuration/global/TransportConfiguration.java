package org.infinispan.configuration.global;

import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.remoting.transport.Transport;

public class TransportConfiguration {
   public static final AttributeDefinition<String> CLUSTER_NAME = AttributeDefinition.builder("cluster", "ISPN")
         .immutable().build();
   public static final AttributeDefinition<String> MACHINE_ID = AttributeDefinition.builder("machine", null, String.class)
         .immutable().build();
   public static final AttributeDefinition<String> RACK_ID = AttributeDefinition.builder("rack", null, String.class)
         .immutable().build();
   public static final AttributeDefinition<String> SITE_ID = AttributeDefinition.builder("site", null, String.class)
         .immutable().build();
   public static final AttributeDefinition<String> NODE_NAME = AttributeDefinition.builder("nodeName", null, String.class)
         .immutable().build();
   public static final AttributeDefinition<TimeQuantity> DISTRIBUTED_SYNC_TIMEOUT = AttributeDefinition.builder(
         "lockTimeout", TimeQuantity.valueOf("4m")).build();
   public static final AttributeDefinition<Integer> INITIAL_CLUSTER_SIZE = AttributeDefinition.builder("initialClusterSize", -1)
         .immutable().build();
   public static final AttributeDefinition<TimeQuantity> INITIAL_CLUSTER_TIMEOUT = AttributeDefinition.builder(
           "initialClusterTimeout", TimeQuantity.valueOf("1m")).parser(TimeQuantity.PARSER).build();
   public static final AttributeDefinition<String> STACK = AttributeDefinition.builder("stack", null, String.class).build();
   public static final AttributeDefinition<String> TRANSPORT_EXECUTOR = AttributeDefinition.builder("executor", "transport-pool", String.class).build();
   public static final AttributeDefinition<String> REMOTE_EXECUTOR = AttributeDefinition.builder("remoteCommandExecutor", "remote-command-pool", String.class).build();
   @SuppressWarnings("unchecked")
   public static final AttributeDefinition<Set<String>> RAFT_MEMBERS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.RAFT_MEMBERS, null, (Class<Set<String>>) (Class<?>) Set.class)
         .initializer(Collections::emptySet)
         // unable to use AttributeSerializer.STRING_COLLECTION because it breaks the parser for JSON and YAML
         .serializer((writer, name, value) -> writer.writeAttribute(name, String.join(" ", value)))
         .immutable()
         .build();

   static AttributeSet attributeSet() {
      return new AttributeSet(TransportConfiguration.class, CLUSTER_NAME, MACHINE_ID, RACK_ID, SITE_ID, NODE_NAME,
            DISTRIBUTED_SYNC_TIMEOUT, INITIAL_CLUSTER_SIZE, INITIAL_CLUSTER_TIMEOUT, STACK, TRANSPORT_EXECUTOR, REMOTE_EXECUTOR,
            RAFT_MEMBERS);
   }

   private final Attribute<String> clusterName;
   private final Attribute<String> stack;
   private final Attribute<String> machineId;
   private final Attribute<String> rackId;
   private final Attribute<String> siteId;
   private final Attribute<String> nodeName;
   private final Attribute<TimeQuantity> distributedSyncTimeout;
   private final Attribute<Integer> initialClusterSize;
   private final Attribute<TimeQuantity> initialClusterTimeout;
   private final AttributeSet attributes;
   private final JGroupsConfiguration jgroupsConfiguration;
   private final TypedProperties properties;

   TransportConfiguration(AttributeSet attributes,
                          JGroupsConfiguration jgroupsConfiguration,
                          TypedProperties properties) {
      this.attributes = attributes.checkProtection();
      this.jgroupsConfiguration = jgroupsConfiguration;
      this.properties = properties;
      clusterName = attributes.attribute(CLUSTER_NAME);
      stack = attributes.attribute(STACK);
      machineId = attributes.attribute(MACHINE_ID);
      rackId = attributes.attribute(RACK_ID);
      siteId = attributes.attribute(SITE_ID);
      distributedSyncTimeout = attributes.attribute(DISTRIBUTED_SYNC_TIMEOUT);
      initialClusterSize = attributes.attribute(INITIAL_CLUSTER_SIZE);
      initialClusterTimeout = attributes.attribute(INITIAL_CLUSTER_TIMEOUT);
      nodeName = attributes.attribute(NODE_NAME);
   }

   public String clusterName() {
      return clusterName.get();
   }

   public String stack() {
      return stack.get();
   }

   public String machineId() {
      return machineId.get();
   }

   public String rackId() {
      return rackId.get();
   }

   public String siteId() {
      return siteId.get();
   }

   public long distributedSyncTimeout() {
      return distributedSyncTimeout.get().longValue();
   }

   public void distributedSyncTimeout(long l) {
      distributedSyncTimeout.set(TimeQuantity.valueOf(l));
   }

   public void distributedSyncTimeout(String s) {
      distributedSyncTimeout.set(TimeQuantity.valueOf(s));
   }

   public int initialClusterSize() {
      return initialClusterSize.get();
   }

   public long initialClusterTimeout() {
      return initialClusterTimeout.get().longValue();
   }

   public Transport transport() {
      return jgroupsConfiguration.transport();
   }

   public String nodeName() {
      return nodeName.get();
   }

   public TypedProperties properties() {
      return properties;
   }

   public boolean hasTopologyInfo() {
      return siteId() != null || rackId() != null || machineId() != null;
   }

   public JGroupsConfiguration jgroups() {
      return jgroupsConfiguration;
   }

   public Set<String> raftMembers() {
      return attributes.attribute(RAFT_MEMBERS).get();
   }

   public AttributeSet attributes() {
      return attributes;
   }
}
