/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.jboss.as.clustering.infinispan.subsystem;

import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;

import org.infinispan.server.jgroups.spi.ChannelFactory;
import org.jboss.modules.ModuleLoader;
import org.jgroups.Channel;

/**
 * @author Paul Ferraro
 */
public class EmbeddedCacheManagerConfigurationService {

    interface TransportConfiguration {
        Long getLockTimeout();
        ChannelFactory getChannelFactory();
        Channel getChannel();
        boolean isStrictPeerToPeer();
        int getInitialClusterSize();
        long getInitialClusterTimeout();
    }

    interface AuthorizationConfiguration {
        String getPrincipalMapper();
        String getAuditLogger();
        Map<String, List<String>> getRoles();
    }

    interface GlobalStateLocationConfiguration {
        String getPersistencePath();
        String getPersistenceRelativeTo();
        String getTemporaryPath();
        String getTemporaryRelativeTo();
    }

    interface Dependencies {
        ModuleLoader getModuleLoader();
        TransportConfiguration getTransportConfiguration();
        AuthorizationConfiguration getAuthorizationConfiguration();
        MBeanServer getMBeanServer();
    }
}
