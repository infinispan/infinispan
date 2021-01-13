package org.infinispan.remoting.transport.jgroups;

import static org.infinispan.util.logging.Log.CONFIG;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.util.Util;
import org.jgroups.JChannel;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.StackType;

/**
 * A JGroups {@link ProtocolStackConfigurator} which
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class EmbeddedJGroupsChannelConfigurator extends AbstractJGroupsChannelConfigurator {

   private static final String PROTOCOL_PREFIX = "org.jgroups.protocols.";

   private final String name;
   final List<ProtocolConfiguration> stack;
   final Map<String, RemoteSite> remoteSites;

   public EmbeddedJGroupsChannelConfigurator(String name, List<ProtocolConfiguration> stack) {
      this.name = name;
      this.stack = stack;
      this.remoteSites = new HashMap<>(2);
   }

   public EmbeddedJGroupsChannelConfigurator(String name) {
      this(name, new ArrayList<>());
   }

   @Override
   public String getProtocolStackString() {
      return stack.toString();
   }

   @Override
   public List<ProtocolConfiguration> getProtocolStack() {
      return stack;
   }

   public String getName() {
      return name;
   }

   @Override
   public JChannel createChannel(String name) throws Exception {
      StackType stackType = org.jgroups.util.Util.getIpStackType();
      List<Protocol> protocols = new ArrayList<>(stack.size());
      for(ProtocolConfiguration c : stack) {
         Protocol protocol;
         try {
            String className = PROTOCOL_PREFIX + c.getProtocolName();
            protocol = Util.getInstanceStrict(className, this.getClass().getClassLoader());
         } catch (ClassNotFoundException e) {
            protocol = Util.getInstanceStrict(c.getProtocolName(), this.getClass().getClassLoader());
         }
         ProtocolConfiguration configuration = new ProtocolConfiguration(protocol.getName(), c.getProperties());
         Configurator.initializeAttrs(protocol, configuration, stackType);
         protocols.add(protocol);

         if (protocol instanceof RELAY2) {
            // Process remote sites if any
            RELAY2 relay2 = (RELAY2) protocol;
            if (relay2 != null) {
               if (remoteSites.size() == 0) {
                  throw CONFIG.jgroupsRelayWithoutRemoteSites(name);
               }
               for (Map.Entry<String, RemoteSite> remoteSite : remoteSites.entrySet()) {
                  JGroupsChannelConfigurator configurator = remoteSite.getValue().configurator;
                  SocketFactory socketFactory = getSocketFactory();
                  final String remoteCluster = remoteSite.getValue().cluster;
                  if (socketFactory instanceof NamedSocketFactory) {
                     // Create a new NamedSocketFactory using the remote cluster name
                     socketFactory = new NamedSocketFactory((NamedSocketFactory) socketFactory, remoteCluster);
                  }
                  configurator.setSocketFactory(socketFactory);
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

            } else {
               if (remoteSites.size() > 0) {
                  throw CONFIG.jgroupsRemoteSitesWithoutRelay(name);
               }
            }
         }
      }

      return applySocketFactory(new JChannel(protocols));
   }

   public void addRemoteSite(String remoteSite, String cluster, JGroupsChannelConfigurator stackConfigurator) {
      if (remoteSites.containsKey(remoteSite)) {
         throw CONFIG.duplicateRemoteSite(remoteSite, name);
      } else {
         remoteSites.put(remoteSite, new RemoteSite(cluster, stackConfigurator));
      }
   }

   public static EmbeddedJGroupsChannelConfigurator combine(JGroupsChannelConfigurator baseStack, EmbeddedJGroupsChannelConfigurator stack) {
      List<ProtocolConfiguration> actualStack = new ArrayList<>();
      // We copy the protocols and properties from the base stack
      for (ProtocolConfiguration originalProtocol : baseStack.getProtocolStack()) {
         ProtocolConfiguration protocol = new ProtocolConfiguration(originalProtocol.getProtocolName(), new HashMap<>(originalProtocol.getProperties()));
         actualStack.add(protocol);
      }
      // We process this stack's rules
      for (ProtocolConfiguration protocol : stack.stack) {
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
      EmbeddedJGroupsChannelConfigurator newStack = new EmbeddedJGroupsChannelConfigurator(stack.getName(), actualStack);
      newStack.remoteSites.putAll(stack.remoteSites);
      return newStack;
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

   public static class RemoteSite {
      final String cluster;
      final JGroupsChannelConfigurator configurator;

      public RemoteSite(String cluster, JGroupsChannelConfigurator configurator) {
         this.cluster = cluster;
         this.configurator = configurator;
      }
   }
}
