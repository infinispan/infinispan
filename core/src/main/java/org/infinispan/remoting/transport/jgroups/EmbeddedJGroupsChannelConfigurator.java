package org.infinispan.remoting.transport.jgroups;

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
import org.jgroups.stack.Protocol;

/**
 * A JGroups {@link ProtocolStackConfigurator} which
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class EmbeddedJGroupsChannelConfigurator implements JGroupsChannelConfigurator {
   private final String name;
   final List<ProtocolConfiguration> stack;
   final Map<String, JGroupsChannelConfigurator> remoteSites;

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

   public Map<String, JGroupsChannelConfigurator> getRemoteSites() {
      return remoteSites;
   }

   public String getName() {
      return name;
   }

   @Override
   public JChannel createChannel() throws Exception {
      List<Protocol> protocols = new ArrayList<>(stack.size());
      for(ProtocolConfiguration c : stack) {
         String className = ProtocolConfiguration.protocol_prefix + "." + c.getProtocolName();
         Protocol protocol = Util.getInstanceStrict(className, this.getClass().getClassLoader());
         protocol.setProperties(c.getProperties());
         protocols.add(protocol);

         if (protocol instanceof RELAY2) {
            // Process remote sites if any
            RELAY2 relay2 = (RELAY2) protocol;
            if (relay2 != null) {
               if (remoteSites.size() == 0) {
                  throw JGroupsTransport.log.jgroupsRelayWithoutRemoteSites(name);
               }
               for (Map.Entry<String, JGroupsChannelConfigurator> remoteSite : remoteSites.entrySet()) {
                  JGroupsChannelConfigurator remoteSiteChannel = remoteSite.getValue();
                  RelayConfig.SiteConfig siteConfig = new RelayConfig.SiteConfig(remoteSite.getKey());
                  siteConfig.addBridge(new RelayConfig.BridgeConfig(remoteSiteChannel.getName()) {
                     @Override
                     public JChannel createChannel() throws Exception {
                        return remoteSiteChannel.createChannel();
                     }
                  });
                  relay2.addSite(remoteSite.getKey(), siteConfig);
               }

            } else {
               if (remoteSites.size() > 0) {
                  throw JGroupsTransport.log.jgroupsRemoteSitesWithoutRelay(name);
               }
            }
         }
      }

      return new JChannel(protocols);
   }

   public void addRemoteSite(String remoteSite, JGroupsChannelConfigurator stackConfigurator) {
      if (remoteSites.containsKey(remoteSite)) {
         throw JGroupsTransport.log.duplicateRemoteSite(remoteSite, name);
      } else {
         remoteSites.put(remoteSite, stackConfigurator);
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
            case INSERT_AFTER:
               if (stackPosition == null) {
                  throw JGroupsTransport.log.jgroupsInsertAfterRequiresPosition(protocolName);
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
         throw JGroupsTransport.log.jgroupsNoStackPosition(mode.name());
      }
   }

   private static void assertExisting(EmbeddedJGroupsChannelConfigurator.StackCombine mode, String protocolName, int position) {
      if (position < 0) {
         throw JGroupsTransport.log.jgroupsNoSuchProtocol(protocolName, mode.name());
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
      REPLACE,
      REMOVE,
      APPEND, // non-public
   }
}
