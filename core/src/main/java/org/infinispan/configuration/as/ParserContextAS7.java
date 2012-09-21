/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.configuration.as;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.configuration.parsing.AbstractParserContext;

/**
 * ParserContextAS7. Holds parsing context for the AS7 parser.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ParserContextAS7 extends AbstractParserContext {
   private final Map<String, NetworkInterface> interfaces;
   private final Map<String, SocketBindingGroup> socketBindingGroups;
   private SocketBindingGroup currentSocketBindingGroup;

   public ParserContextAS7() {
      interfaces = new HashMap<String, NetworkInterface>();
      socketBindingGroups = new HashMap<String, SocketBindingGroup>();
   }

   public void addInterface(NetworkInterface networkInterface) {
      interfaces.put(networkInterface.name(), networkInterface);
   }

   public NetworkInterface getInterface(String name) {
      return interfaces.get(name);
   }

   public void addSocketBindingGroup(SocketBindingGroup socketBindingGroup) {
      socketBindingGroups.put(socketBindingGroup.name(), socketBindingGroup);
      currentSocketBindingGroup = socketBindingGroup;
   }

   public SocketBindingGroup getSocketBindingGroup(String name) {
      return socketBindingGroups.get(name);
   }

   public SocketBindingGroup getCurrentSocketBindingGroup() {
      return currentSocketBindingGroup;
   }

   /**
    * Searches for the named {@link OutboundSocketBinding} in all {@link SocketBindingGroup}s
    */
   public OutboundSocketBinding getOutboundSocketBinding(String name) {
      for (SocketBindingGroup group : socketBindingGroups.values()) {
         OutboundSocketBinding binding = group.getOutboundSocketBinding(name);
         if (binding != null)
            return binding;
      }
      return null;
   }

   /**
    * Searches for the named {@link SocketBinding} in all {@link SocketBindingGroup}s
    */
   public SocketBinding getSocketBinding(String name) {
      for (SocketBindingGroup group : socketBindingGroups.values()) {
         SocketBinding binding = group.getSocketBinding(name);
         if (binding != null)
            return binding;
      }
      return null;
   }

}
