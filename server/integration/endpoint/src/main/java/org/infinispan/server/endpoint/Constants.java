/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011-2013 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.server.endpoint;

import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.server.deployment.AttachmentKey;
import org.jboss.as.server.deployment.AttachmentList;
import org.jboss.msc.service.ServiceName;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

/**
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author Tristan Tarrant
 */
public class Constants {

   private static final ServiceName JBOSS = ServiceName.of("jboss");
   public static final String SUBSYSTEM_NAME = "endpoint";

   public static final ServiceName DATAGRID = JBOSS.append(SUBSYSTEM_NAME);
   public static final PathElement SUBSYSTEM_PATH = PathElement.pathElement(ModelDescriptionConstants.SUBSYSTEM, SUBSYSTEM_NAME);
   public static final ServiceName EXTENSION_MANAGER_NAME = DATAGRID.append("extension-manager");

   public static final int INSTALL_FILTER_FACTORY = 0x1801;
   public static final int INSTALL_CONVERTER_FACTORY = 0x1802;
   public static final int INSTALL_MARSHALLER = 0x1803;
   public static final int DEPENDENCIES = 0x1C01;

   public static String VERSION = Constants.class.getPackage().getImplementationVersion();

    public static final AttachmentKey<AttachmentList<ServiceName>> FILTER_FACTORY_SERVICES = AttachmentKey.createList(ServiceName.class);

   private Constants() {
      // Constant table
   }

   public static <T> T notNull(T value) {
      if (value == null)
         throw ROOT_LOGGER.serviceNotStarted();
      return value;
   }

}
