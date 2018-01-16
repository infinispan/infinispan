/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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

import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.registry.ManagementResourceRegistration;

public class MultiTenancyResource extends SimpleResourceDefinition {

   public static final PathElement MULTI_TENANCY_PATH = PathElement.pathElement(ModelKeys.MULTI_TENANCY);

   public MultiTenancyResource() {
      super(MULTI_TENANCY_PATH, EndpointExtension.getResourceDescriptionResolver(ModelKeys.MULTI_TENANCY), MultiTenancySubsystemAdd.INSTANCE, ReloadRequiredRemoveStepHandler.INSTANCE);
   }

   @Override
   public void registerChildren(ManagementResourceRegistration resourceRegistration) {
      resourceRegistration.registerSubModel(new MultiTenantHotRodResource());
      resourceRegistration.registerSubModel(new MultiTenantRestResource());
   }

}
