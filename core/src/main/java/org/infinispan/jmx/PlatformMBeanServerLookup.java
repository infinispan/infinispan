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
package org.infinispan.jmx;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.Properties;

/**
 * Default implementation for {@link MBeanServerLookup}, will return the platform MBean server.
 * <p/>
 * Note: to enable platform MBeanServer the following system property should be passed to the Sun JVM:
 * <b>-Dcom.sun.management.jmxremote</b>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PlatformMBeanServerLookup implements MBeanServerLookup {

   public MBeanServer getMBeanServer(Properties properties) {
      return ManagementFactory.getPlatformMBeanServer();
   }

}
