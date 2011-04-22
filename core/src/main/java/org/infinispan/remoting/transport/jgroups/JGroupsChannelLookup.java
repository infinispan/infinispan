/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.remoting.transport.jgroups;

import org.jgroups.Channel;

import java.util.Properties;

/**
 * A hook to pass in a JGroups channel.  Implementations need to provide a public no-arg constructor as instances are
 * created via reflection.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface JGroupsChannelLookup {
   /**
    * Retrieves a JGroups channel.  Passes in all of the properties used to configure the channel.
    * @param p properties
    * @return a JGroups channel
    */
   Channel getJGroupsChannel(Properties p);

   /**
    * @return true if the JGroupsTransport should start and connect the channel before using it; false if the transport
    * should assume that the channel is connected and started.
    */
   boolean shouldStartAndConnect();

   /**
    * @return true if the JGroupsTransport should stop and disconnect the channel once it is done with it; false if
    * the channel is to be left open/connected.
    */
   boolean shouldStopAndDisconnect();
}
