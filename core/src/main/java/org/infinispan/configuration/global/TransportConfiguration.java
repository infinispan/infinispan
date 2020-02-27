package org.infinispan.configuration.global;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.remoting.transport.Transport;

public class TransportConfiguration implements ConfigurationInfo {
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
   public static final AttributeDefinition<Long> DISTRIBUTED_SYNC_TIMEOUT = AttributeDefinition.builder(
         "lockTimeout", TimeUnit.MINUTES.toMillis(4)).build();
   public static final AttributeDefinition<Integer> INITIAL_CLUSTER_SIZE = AttributeDefinition.builder("initialClusterSize", -1)
         .immutable().build();
   public static final AttributeDefinition<Long> INITIAL_CLUSTER_TIMEOUT = AttributeDefinition.builder(
           "initialClusterTimeout", TimeUnit.MINUTES.toMillis(1)).build();
   public static final AttributeDefinition<String> STACK = AttributeDefinition.builder("stack", null, String.class).build();
   public static final AttributeDefinition<String> TRANSPORT_EXECUTOR = AttributeDefinition.builder("executor", "transport-pool", String.class).build();
   public static final AttributeDefinition<String> REMOTE_EXECUTOR = AttributeDefinition.builder("remoteCommandExecutor", "remote-command-pool", String.class).build();

   static AttributeSet attributeSet() {
      return new AttributeSet(TransportConfiguration.class, CLUSTER_NAME, MACHINE_ID, RACK_ID, SITE_ID, NODE_NAME,
            DISTRIBUTED_SYNC_TIMEOUT, INITIAL_CLUSTER_SIZE, INITIAL_CLUSTER_TIMEOUT, STACK, TRANSPORT_EXECUTOR, REMOTE_EXECUTOR);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.TRANSPORT.getLocalName());

   private final Attribute<String> clusterName;
   private final Attribute<String> machineId;
   private final Attribute<String> rackId;
   private final Attribute<String> siteId;
   private final Attribute<String> nodeName;
   private final Attribute<Long> distributedSyncTimeout;
   private final Attribute<Integer> initialClusterSize;
   private final Attribute<Long> initialClusterTimeout;
   private final AttributeSet attributes;
   private final JGroupsConfiguration jgroupsConfiguration;
   private final ThreadPoolConfiguration transportThreadPool;
   private final ThreadPoolConfiguration remoteCommandThreadPool;
   private final TypedProperties properties;

   TransportConfiguration(AttributeSet attributes,
                          JGroupsConfiguration jgroupsConfiguration,
                          ThreadPoolConfiguration transportThreadPool,
                          ThreadPoolConfiguration remoteCommandThreadPool,
                          TypedProperties properties) {
      this.attributes = attributes.checkProtection();
      this.jgroupsConfiguration = jgroupsConfiguration;
      this.transportThreadPool = transportThreadPool;
      this.remoteCommandThreadPool = remoteCommandThreadPool;
      this.properties = properties;
      clusterName = attributes.attribute(CLUSTER_NAME);
      machineId = attributes.attribute(MACHINE_ID);
      rackId = attributes.attribute(RACK_ID);
      siteId = attributes.attribute(SITE_ID);
      distributedSyncTimeout = attributes.attribute(DISTRIBUTED_SYNC_TIMEOUT);
      initialClusterSize = attributes.attribute(INITIAL_CLUSTER_SIZE);
      initialClusterTimeout = attributes.attribute(INITIAL_CLUSTER_TIMEOUT);
      nodeName = attributes.attribute(NODE_NAME);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public String clusterName() {
      return clusterName.get();
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
      return distributedSyncTimeout.get();
   }

   public int initialClusterSize() {
      return initialClusterSize.get();
   }

   public long initialClusterTimeout() {
      return initialClusterTimeout.get();
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

   @Deprecated
   public ThreadPoolConfiguration transportThreadPool() {
      return transportThreadPool;
   }

   @Deprecated
   public ThreadPoolConfiguration remoteCommandThreadPool() {
      return remoteCommandThreadPool;
   }

   public String transportThreadPoolName() {
      return attributes.attribute(TRANSPORT_EXECUTOR).get();
   }

   public String remoteThreadPoolName() {
      return attributes.attribute(REMOTE_EXECUTOR).get();
   }

   public JGroupsConfiguration jgroups() {
      return jgroupsConfiguration;
   }

   public AttributeSet attributes() {
      return attributes;
   }
}
