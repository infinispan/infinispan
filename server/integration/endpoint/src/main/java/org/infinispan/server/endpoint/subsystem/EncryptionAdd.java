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

import java.util.List;

import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

/**
 * SecurityAdd.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class EncryptionAdd extends AbstractAddStepHandler {
   static final EncryptionAdd INSTANCE = new EncryptionAdd(EncryptionResource.ENCRYPTION_ATTRIBUTES);

   private final AttributeDefinition[] attributes;

   EncryptionAdd(final AttributeDefinition[] attributes) {
      this.attributes = attributes;
   }

   @Override
   protected void populateModel(ModelNode operation, ModelNode model) throws OperationFailedException {
      for (AttributeDefinition attr : attributes) {
         attr.validateAndSet(operation, model);
      }
   }

   @Override
   protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {
      super.performRuntime(context, operation, model);
      // once we add a cache configuration, we need to restart all the services for the changes to take effect
      context.reloadRequired();
   }
}
