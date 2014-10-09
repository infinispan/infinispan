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
package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.commons.util.Util;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

import java.net.URL;
import java.util.List;

import static org.jboss.as.clustering.infinispan.InfinispanMessages.MESSAGES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

/**
 * Handler that performs the operation of uploading a protobuf file to be used.
 *
 * @author William Burns
 * @author gustavonalle
 * @since 6.0
 */
public class UploadProtoFileOperationHandler implements OperationStepHandler {

    public static final UploadProtoFileOperationHandler INSTANCE = new UploadProtoFileOperationHandler();

    @Override
    public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
        final String namesParameter = CacheContainerResource.PROTO_NAMES.getName();
        final String urlsParameter = CacheContainerResource.PROTO_URLS.getName();
        final ModelNode names = operation.require(namesParameter);
        final ModelNode urls = operation.require(urlsParameter);

        final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
        final String cacheContainerName = address.getElement(address.size() - 1).getValue();
        final ServiceController<?> controller = context.getServiceRegistry(false).getService(
              EmbeddedCacheManagerService.getServiceName(cacheContainerName));

        EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) controller.getValue();
        ProtobufMetadataManager protoManager = cacheManager.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
        if (protoManager != null) {
           try {
              List<ModelNode> descriptorsNames = names.asList();
              List<ModelNode> descriptorsUrls = urls.asList();
              if (descriptorsNames.size() != descriptorsUrls.size()) {
                 throw MESSAGES.invalidParameterSizes(namesParameter, urlsParameter);
              }
              String[] nameArray = new String[descriptorsNames.size()];
              String[] contentArray = new String[descriptorsUrls.size()];
              int i = 0;
              for (ModelNode modelNode : descriptorsNames) {
                 nameArray[i] = modelNode.asString();
                 String urlString = descriptorsUrls.get(i).asString();
                 contentArray[i] = Util.read(new URL(urlString).openStream());
                 i++;   
              }
              protoManager.registerProtofiles(nameArray, contentArray);
           } catch (Exception e) {
              throw new OperationFailedException(new ModelNode().set(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage())));
           }
        }
        context.stepCompleted();
    }
}
