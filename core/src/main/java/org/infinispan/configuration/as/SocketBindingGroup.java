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

public class SocketBindingGroup {
   private final String name;
   private final String defaultInterface;
   private final int portOffset;
   private final Map<String, SocketBinding> socketBindings;
   private final Map<String, OutboundSocketBinding> outboundSocketBindings;

   public SocketBindingGroup(String name, String defaultInterface, int portOffset) {
      this.name = name;
      this.defaultInterface = defaultInterface;
      this.portOffset = portOffset;
      this.socketBindings = new HashMap<String, SocketBinding>();
      this.outboundSocketBindings = new HashMap<String, OutboundSocketBinding>();
   }

   public void addSocketBinding(SocketBinding socketBinding) {
      socketBindings.put(socketBinding.name(), socketBinding);
   }

   public void addOutboundSocketBinding(OutboundSocketBinding outboundSocketBinding) {
      outboundSocketBindings.put(outboundSocketBinding.name(), outboundSocketBinding);
   }

   public SocketBinding getSocketBinding(String name) {
      return socketBindings.get(name);
   }

   public OutboundSocketBinding getOutboundSocketBinding(String name) {
      return outboundSocketBindings.get(name);
   }

   public String name() {
      return name;
   }

   public String defaultInterface() {
      return defaultInterface;
   }

   public int portOffset() {
      return portOffset;
   }

   @Override
   public String toString() {
      return "SocketBindingGroup [name=" + name + ", defaultInterface=" + defaultInterface + ", portOffset=" + portOffset + ", socketBindings=" + socketBindings
            + ", outboundSocketBindings=" + outboundSocketBindings + "]";
   }

}
