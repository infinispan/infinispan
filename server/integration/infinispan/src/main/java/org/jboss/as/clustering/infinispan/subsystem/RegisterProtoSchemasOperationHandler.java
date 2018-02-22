/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
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

import static org.jboss.as.clustering.infinispan.InfinispanMessages.MESSAGES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.List;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;

/**
 * Handler to register the proto file(s) contents via DMR/CLI
 *
 * @author gustavonalle
 * @since 7.0
 */
public class RegisterProtoSchemasOperationHandler implements OperationStepHandler {

   public static final RegisterProtoSchemasOperationHandler INSTANCE = new RegisterProtoSchemasOperationHandler();

   @Override
   public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
      final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
      final String cacheContainerName = address.getElement(address.size() - 1).getValue();
      final ServiceController<?> controller = context.getServiceRegistry(false).getService(
              CacheContainerServiceName.CACHE_CONTAINER.getServiceName(cacheContainerName));

      if (controller != null) {
         EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) controller.getValue();
         ProtobufMetadataManager protoManager = cacheManager.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
         if (protoManager != null) {
            try {
               String namesParameter = CacheContainerResource.PROTO_NAMES.getName();
               String contentsParameter = CacheContainerResource.PROTO_CONTENTS.getName();
               ModelNode names = operation.require(namesParameter);
               ModelNode contents = operation.require(contentsParameter);
               validateParameters(names, contents);
               List<ModelNode> descriptorsNames = names.asList();
               List<ModelNode> descriptorsContents = contents.asList();
               String[] nameArray = new String[descriptorsNames.size()];
               String[] contentArray = new String[descriptorsNames.size()];
               int i = 0;
               for (ModelNode modelNode : descriptorsNames) {
                  nameArray[i] = modelNode.asString();
                  contentArray[i] = descriptorsContents.get(i).asString();
                  i++;
               }
               protoManager.registerProtofiles(nameArray, contentArray);
            } catch (Exception e) {
               throw new OperationFailedException(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage()));
            }
         }
      }
   }

   private void validateParameters(ModelNode name, ModelNode contents) {
      String requiredType = ModelType.LIST.toString();
      String nameParameter = CacheContainerResource.PROTO_NAMES.getName();
      String contentParameter = CacheContainerResource.PROTO_CONTENTS.getName();
      if (name.getType() != ModelType.LIST) {
         throw MESSAGES.invalidParameterType(nameParameter, requiredType);
      }
      if (contents.getType() != ModelType.LIST) {
         throw MESSAGES.invalidParameterType(contentParameter, requiredType);
      }
      if (name.asList().size() != contents.asList().size()) {
         throw MESSAGES.invalidParameterSizes(nameParameter, contentParameter);
      }
   }

}
