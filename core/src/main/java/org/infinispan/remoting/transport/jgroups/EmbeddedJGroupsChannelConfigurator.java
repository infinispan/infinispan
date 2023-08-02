package org.infinispan.remoting.transport.jgroups;

import static org.infinispan.util.logging.Log.CONFIG;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.configuration.global.JGroupsConfiguration;
import org.infinispan.xsite.XSiteNamedCache;
import org.jgroups.ChannelListener;
import org.jgroups.JChannel;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.Protocol;
import org.jgroups.util.SocketFactory;

/**
 * A JGroups {@link ProtocolStackConfigurator} which
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class EmbeddedJGroupsChannelConfigurator extends AbstractJGroupsChannelConfigurator {

   private final String name;
   private final String parent;
   private JGroupsConfiguration jgroupsConfiguration;
   private final List<ProtocolConfiguration> stack;
   private final RemoteSites remoteSites;

   public EmbeddedJGroupsChannelConfigurator(String name, List<ProtocolConfiguration> stack, RemoteSites remoteSites) {
      this(name, stack, remoteSites, null);
   }

   public EmbeddedJGroupsChannelConfigurator(String name, List<ProtocolConfiguration> stack, RemoteSites remoteSites, String parent) {
      this.name = name;
      this.stack = stack;
      this.remoteSites = remoteSites;
      this.parent = parent;
   }

   public void setConfiguration(JGroupsConfiguration configuration) {
      jgroupsConfiguration = configuration;
   }

   @Override
   public String getProtocolStackString() {
      return getProtocolStack().toString();
   }

   @Override
   public List<ProtocolConfiguration> getProtocolStack() {
      return combineStack(jgroupsConfiguration.configurator(parent), stack);
   }

   public List<ProtocolConfiguration> getUncombinedProtocolStack() {
      return stack;
   }

   public String getName() {
      return name;
   }

   @Override
   public JChannel createChannel(String name) throws Exception {
      if (hasSites() && getProtocolStack().stream().noneMatch(EmbeddedJGroupsChannelConfigurator::isRelay2)) {
         throw CONFIG.jgroupsRemoteSitesWithoutRelay(name);
      }
      return amendChannel(new JChannel(this));
   }

   @Override
   public void afterCreation(Protocol protocol) {
      if (!(protocol instanceof RELAY2)) {
         return;
      }
      // Process remote sites if any
      RemoteSites actualSites = getRemoteSites();
      if (actualSites.remoteSites.size() == 0) {
         throw CONFIG.jgroupsRelayWithoutRemoteSites(name);
      }
      RELAY2 relay2 = (RELAY2) protocol;
      for (Map.Entry<String, RemoteSite> remoteSite : actualSites.remoteSites.entrySet()) {
         JGroupsChannelConfigurator configurator = jgroupsConfiguration.configurator(remoteSite.getValue().stack);
         SocketFactory socketFactory = getSocketFactory();
         String remoteCluster = remoteSite.getValue().cluster;
         if (remoteCluster == null) {
            remoteCluster = actualSites.defaultCluster;
         }
         if (socketFactory instanceof NamedSocketFactory) {
            // Create a new NamedSocketFactory using the remote cluster name
            socketFactory = new NamedSocketFactory((NamedSocketFactory) socketFactory, remoteCluster);
         }
         configurator.setSocketFactory(socketFactory);
         for(ChannelListener listener : channelListeners) {
            configurator.addChannelListener(listener);
         }
         RelayConfig.SiteConfig siteConfig = new RelayConfig.SiteConfig(remoteSite.getKey());
         siteConfig.addBridge(new RelayConfig.BridgeConfig(remoteCluster) {
            @Override
            public JChannel createChannel() throws Exception {
               // TODO The bridge channel is created lazily, and Infinispan doesn't see any errors
               return configurator.createChannel(getClusterName());
            }
         });
         relay2.addSite(remoteSite.getKey(), siteConfig);
      }
   }

   private static List<ProtocolConfiguration> combineStack(JGroupsChannelConfigurator baseStack, List<ProtocolConfiguration> stack) {
      List<ProtocolConfiguration> actualStack = new ArrayList<>(stack.size());
      if (baseStack != null) {
         // We copy the protocols and properties from the base stack. This will recursively perform inheritance
         for (ProtocolConfiguration originalProtocol : baseStack.getProtocolStack()) {
            ProtocolConfiguration protocol = new ProtocolConfiguration(originalProtocol.getProtocolName(), new HashMap<>(originalProtocol.getProperties()));
            actualStack.add(protocol);
         }
      }
      // We process this stack's rules
      for (ProtocolConfiguration protocol : stack) {
         String protocolName = protocol.getProtocolName();
         int position = findProtocol(protocolName, actualStack);
         EmbeddedJGroupsChannelConfigurator.StackCombine mode = position < 0 ? StackCombine.APPEND : StackCombine.COMBINE;
         // See if there is a "stack.*" strategy
         String stackCombine = protocol.getProperties().remove("stack.combine");
         if (stackCombine != null) {
            mode = EmbeddedJGroupsChannelConfigurator.StackCombine.valueOf(stackCombine);
         }
         String stackPosition = protocol.getProperties().remove("stack.position");

         switch (mode) {
            case APPEND:
               assertNoStackPosition(mode, stackPosition);
               actualStack.add(protocol);
               break;
            case COMBINE:
               assertNoStackPosition(mode, stackPosition);
               assertExisting(mode, protocolName, position);
               // Combine/overwrite properties
               actualStack.get(position).getProperties().putAll(protocol.getProperties());
               break;
            case REMOVE:
               assertNoStackPosition(mode, stackPosition);
               assertExisting(mode, protocolName, position);
               actualStack.remove(position);
               break;
            case REPLACE:
               if (stackPosition != null) {
                  position = findProtocol(stackPosition, actualStack);
                  assertExisting(mode, stackPosition, position);
               } else {
                  assertExisting(mode, protocolName, position);
               }
               actualStack.set(position, protocol);
               break;
            case INSERT_BEFORE:
            case INSERT_BELOW:
               if (stackPosition == null) {
                  throw CONFIG.jgroupsInsertRequiresPosition(protocolName);
               }
               position = findProtocol(stackPosition, actualStack);
               assertExisting(mode, stackPosition, position);
               actualStack.add(position, protocol);
               break;
            case INSERT_AFTER:
            case INSERT_ABOVE:
               if (stackPosition == null) {
                  throw CONFIG.jgroupsInsertRequiresPosition(protocolName);
               }
               position = findProtocol(stackPosition, actualStack);
               assertExisting(mode, stackPosition, position);
               actualStack.add(position + 1, protocol);
               break;
         }
      }
      return actualStack;
   }

   private void combineSites(Map<String, RemoteSite> sites) {
      JGroupsChannelConfigurator parentConfigurator = jgroupsConfiguration.configurator(parent);
      if (parentConfigurator instanceof EmbeddedJGroupsChannelConfigurator) {
         ((EmbeddedJGroupsChannelConfigurator) parentConfigurator).combineSites(sites);
      }
      if (remoteSites != null) {
         sites.putAll(remoteSites.remoteSites);
      }
   }

   private boolean hasSites() {
      if (remoteSites != null && !remoteSites.remoteSites.isEmpty()) {
         return true;
      }
      if (parent == null) {
         return false;
      }
      // let's see if the parent has remote sites
      Map<String, RemoteSite> sites = new HashMap<>(4);
      JGroupsChannelConfigurator parentConfigurator = jgroupsConfiguration.configurator(parent);
      if (parentConfigurator instanceof EmbeddedJGroupsChannelConfigurator) {
         ((EmbeddedJGroupsChannelConfigurator) parentConfigurator).combineSites(sites);
      }
      return !sites.isEmpty();
   }

   private static void assertNoStackPosition(EmbeddedJGroupsChannelConfigurator.StackCombine mode, String stackAfter) {
      if (stackAfter != null) {
         throw CONFIG.jgroupsNoStackPosition(mode.name());
      }
   }

   private static void assertExisting(EmbeddedJGroupsChannelConfigurator.StackCombine mode, String protocolName, int position) {
      if (position < 0) {
         throw CONFIG.jgroupsNoSuchProtocol(protocolName, mode.name());
      }
   }

   private static int findProtocol(String protocol, List<ProtocolConfiguration> stack) {
      for (int i = 0; i < stack.size(); i++) {
         if (protocol.equals(stack.get(i).getProtocolName()))
            return i;
      }
      return -1;
   }

   public RemoteSites getRemoteSites() {
      RemoteSites combinedSites = new RemoteSites(remoteSites.defaultStack, remoteSites.defaultCluster);
      combineSites(combinedSites.remoteSites);
      return combinedSites;
   }

   public RemoteSites getUncombinedRemoteSites() {
      return remoteSites;
   }

   @Override
   public String toString() {
      return "EmbeddedJGroupsChannelConfigurator{" +
            "name='" + name + '\'' +
            ", parent='" + parent + '\'' +
            ", stack=" + stack +
            ", remoteSites=" + remoteSites +
            '}';
   }

   private static boolean isRelay2(ProtocolConfiguration configuration) {
      try {
         return configuration.isAssignableProtocol(RELAY2.class, EmbeddedJGroupsChannelConfigurator.class);
      } catch (Exception e) {
         // it is ok to return false, JChannel will throw the exception
         return false;
      }
   }

   public enum StackCombine {
      COMBINE,
      INSERT_AFTER,
      INSERT_ABOVE,
      INSERT_BEFORE,
      INSERT_BELOW,
      REPLACE,
      REMOVE,
      APPEND, // non-public
   }

   public static class RemoteSites {
      final String defaultCluster;
      final String defaultStack;
      final Map<String, RemoteSite> remoteSites;

      public RemoteSites(String defaultStack, String defaultCluster) {
         this.defaultStack = defaultStack;
         this.defaultCluster = defaultCluster;
         remoteSites = new LinkedHashMap<>(4);
      }

      public String getDefaultCluster() {
         return defaultCluster;
      }

      public String getDefaultStack() {
         return defaultStack;
      }

      public Map<String, RemoteSite> getRemoteSites() {
         return remoteSites;
      }

      public void addRemoteSite(String stackName, String remoteSite, String cluster, String stack) {
         remoteSite = XSiteNamedCache.cachedString(remoteSite);
         if (remoteSites.containsKey(remoteSite)) {
            throw CONFIG.duplicateRemoteSite(remoteSite, stackName);
         } else {
            remoteSites.put(remoteSite, new RemoteSite(cluster, stack));
         }
      }

      @Override
      public String toString() {
         return "RemoteSites{" +
               "defaultCluster='" + defaultCluster + '\'' +
               ", defaultStack='" + defaultStack + '\'' +
               ", remoteSites=" + remoteSites +
               '}';
      }
   }

   public static class RemoteSite {
      final String cluster;
      final String stack;

      RemoteSite(String cluster, String stack) {
         this.cluster = cluster;
         this.stack = stack;
      }

      public String getCluster() {
         return cluster;
      }

      public String getStack() {
         return stack;
      }

      @Override
      public String toString() {
         return "RemoteSite{" +
               "cluster='" + cluster + '\'' +
               ", stack='" + stack + '\'' +
               '}';
      }
   }
}
