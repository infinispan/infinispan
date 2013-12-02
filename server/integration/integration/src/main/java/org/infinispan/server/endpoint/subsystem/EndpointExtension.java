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
package org.infinispan.server.endpoint.subsystem;

import java.util.List;

import org.infinispan.server.endpoint.Constants;
import org.jboss.as.controller.Extension;
import org.jboss.as.controller.ExtensionContext;
import org.jboss.as.controller.SubsystemRegistration;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.parsing.ExtensionParsingContext;
import org.jboss.dmr.ModelNode;
import org.jboss.staxmapper.XMLElementReader;


/**
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author Tristan Tarrant
 */
public class EndpointExtension implements Extension {

   private static final int MANAGEMENT_API_MAJOR_VERSION = 1;
   private static final int MANAGEMENT_API_MINOR_VERSION = 2;
   private static final int MANAGEMENT_API_MICRO_VERSION = 0;
   private static final String RESOURCE_NAME = EndpointExtension.class.getPackage().getName() + ".LocalDescriptions";

   static ResourceDescriptionResolver getResourceDescriptionResolver(String keyPrefix) {
      /*StringBuilder prefix = new StringBuilder(Constants.SUBSYSTEM_NAME);
      for (String kp : keyPrefix) {
          prefix.append('.').append(kp);
      }*/
      return new SharedResourceDescriptionResolver(keyPrefix, RESOURCE_NAME, EndpointExtension.class.getClassLoader(), true, true, null);
  }


   @Override
   public final void initialize(ExtensionContext context) {
      final boolean registerRuntimeOnly = context.isRuntimeOnlyRegistrationValid();

      final SubsystemRegistration subsystem = context.registerSubsystem(Constants.SUBSYSTEM_NAME, MANAGEMENT_API_MAJOR_VERSION, MANAGEMENT_API_MINOR_VERSION, MANAGEMENT_API_MICRO_VERSION);
      subsystem.registerSubsystemModel(new EndpointSubsystemRootResource(registerRuntimeOnly));
      subsystem.registerXMLElementWriter(new EndpointSubsystemWriter());
   }

   @Override
   public void initializeParsers(ExtensionParsingContext context) {
       for (Namespace namespace: Namespace.values()) {
           XMLElementReader<List<ModelNode>> reader = namespace.getXMLReader();
           if (reader != null) {
               context.setSubsystemXmlMapping(Constants.SUBSYSTEM_NAME, namespace.getUri(), reader);
           }
       }
   }
}
