package org.infinispan.configuration.global;

import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.remoting.transport.Transport;

public class TransportConfiguration extends ConfigurationElement<TransportConfiguration> {
   public static final AttributeDefinition<String> CLUSTER_NAME = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.CLUSTER, "ISPN")
         .immutable().build();
   public static final AttributeDefinition<String> MACHINE_ID = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MACHINE_ID, null, String.class)
         .immutable().build();
   public static final AttributeDefinition<String> RACK_ID = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.RACK_ID, null, String.class)
         .immutable().build();
   public static final AttributeDefinition<String> SITE_ID = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.SITE, null, String.class)
         .immutable().build();
   public static final AttributeDefinition<String> NODE_NAME = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.NODE_NAME, null, String.class)
         .immutable().build();
   public static final AttributeDefinition<TimeQuantity> DISTRIBUTED_SYNC_TIMEOUT = AttributeDefinition.builder(
         org.infinispan.configuration.parsing.Attribute.LOCK_TIMEOUT, TimeQuantity.valueOf("4m")).build();
   public static final AttributeDefinition<Integer> INITIAL_CLUSTER_SIZE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.INITIAL_CLUSTER_SIZE, -1)
         .immutable().build();
   public static final AttributeDefinition<TimeQuantity> INITIAL_CLUSTER_TIMEOUT = AttributeDefinition.builder(
         org.infinispan.configuration.parsing.Attribute.INITIAL_CLUSTER_TIMEOUT, TimeQuantity.valueOf("1m")).parser(TimeQuantity.PARSER).build();
   public static final AttributeDefinition<String> STACK = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.STACK, null, String.class).build();
   public static final AttributeDefinition<String> TRANSPORT_EXECUTOR = AttributeDefinition.builder("executor", "transport-pool", String.class).autoPersist(false).build();
   public static final AttributeDefinition<String> REMOTE_EXECUTOR = AttributeDefinition.builder("remoteCommandExecutor", "remote-command-pool", String.class).autoPersist(false).build();
   @SuppressWarnings("unchecked")
   public static final AttributeDefinition<Set<String>> RAFT_MEMBERS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.RAFT_MEMBERS, null, (Class<Set<String>>) (Class<?>) Set.class)
         .initializer(Collections::emptySet)
         // unable to use AttributeSerializer.STRING_COLLECTION because it breaks the parser for JSON and YAML
         .serializer((writer, name, value) -> {
         if (!value.isEmpty()) writer.writeAttribute(name, String.join(" ", value));
      })
         .immutable()
         .build();

   static AttributeSet attributeSet() {
      return new AttributeSet(TransportConfiguration.class, CLUSTER_NAME, MACHINE_ID, RACK_ID, SITE_ID, NODE_NAME,
            DISTRIBUTED_SYNC_TIMEOUT, INITIAL_CLUSTER_SIZE, INITIAL_CLUSTER_TIMEOUT, STACK, TRANSPORT_EXECUTOR, REMOTE_EXECUTOR,
            RAFT_MEMBERS);
   }

   private final Attribute<String> clusterName;
   private final Attribute<String> nodeName;
   private final Attribute<TimeQuantity> distributedSyncTimeout;
   private final JGroupsConfiguration jgroupsConfiguration;
   private final TypedProperties properties;

   TransportConfiguration(AttributeSet attributes,
                          JGroupsConfiguration jgroupsConfiguration,
                          TypedProperties properties) {
      super(Element.TRANSPORT, attributes);
      this.jgroupsConfiguration = jgroupsConfiguration;
      this.properties = properties;
      clusterName = attributes.attribute(CLUSTER_NAME);
      distributedSyncTimeout = attributes.attribute(DISTRIBUTED_SYNC_TIMEOUT);
      nodeName = attributes.attribute(NODE_NAME);
   }

   public String clusterName() {
      return clusterName.get();
   }

   public String stack() {
      return attributes.attribute(STACK).get();
   }

   public String machineId() {
      return attributes.attribute(MACHINE_ID).get();
   }

   public String rackId() {
      return attributes.attribute(RACK_ID).get();
   }

   public String siteId() {
      return attributes.attribute(SITE_ID).get();
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
      return attributes.attribute(INITIAL_CLUSTER_SIZE).get();
   }

   public long initialClusterTimeout() {
      return attributes.attribute(INITIAL_CLUSTER_TIMEOUT).get().longValue();
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
}
