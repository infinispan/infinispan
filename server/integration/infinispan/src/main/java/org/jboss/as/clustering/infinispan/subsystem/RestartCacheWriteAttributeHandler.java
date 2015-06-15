package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.server.infinispan.spi.service.CacheServiceName;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.RestartParentWriteAttributeHandler;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;

public class RestartCacheWriteAttributeHandler extends RestartParentWriteAttributeHandler {
    private final CacheAdd cacheAddHandler;

    public RestartCacheWriteAttributeHandler(String parentKeyName, CacheAdd cacheAddHandler, AttributeDefinition... definitions) {
        super(parentKeyName, definitions);
        this.cacheAddHandler = cacheAddHandler;
    }

    @Override
    protected void recreateParentService(OperationContext context, PathAddress cacheAddress, ModelNode cacheModel) throws OperationFailedException {
        recreateParentService(context, cacheAddress, cacheModel, null);
    }

    @Override
    protected void recreateParentService(OperationContext context, PathAddress cacheAddress, ModelNode cacheModel,
            ServiceVerificationHandler verificationHandler) throws OperationFailedException {
        PathAddress containerAddress = cacheAddress.subAddress(0, cacheAddress.size()-1) ;
        ModelNode containerModel = context.readResourceFromRoot(containerAddress).getModel();
        ModelNode operation = Util.createAddOperation(cacheAddress);

        cacheAddHandler.installRuntimeServices(context, operation, containerModel, cacheModel);
    }

    @Override
    protected ServiceName getParentServiceName(PathAddress parentAddress) {
        int position = parentAddress.size();
        PathAddress cacheAddress = parentAddress.subAddress(position - 1);
        PathAddress containerAddress = parentAddress.subAddress(position - 2, position - 1);
        return CacheServiceName.CACHE.getServiceName(containerAddress.getLastElement().getValue(), cacheAddress.getLastElement().getValue());
    }

    @Override
    protected void removeServices(OperationContext context, ServiceName parentService, ModelNode parentModel)
            throws OperationFailedException {
        String containerName = parentService.getParent().getSimpleName();
        String cacheName = parentService.getSimpleName();
        ModelNode resolvedValue = null;
        String jndiName = (resolvedValue = CacheResource.JNDI_NAME.resolveModelAttribute(context, parentModel)).isDefined() ? resolvedValue.asString() : null;
        ContextNames.BindInfo bindInfo;
        bindInfo = ContextNames.bindInfoFor(InfinispanJndiName.createCacheJndiName(jndiName, containerName, cacheName));
        context.removeService(bindInfo.getBinderServiceName());
        super.removeServices(context, parentService, parentModel);
        context.removeService(CacheServiceName.CONFIGURATION.getServiceName(containerName, cacheName));
    }

    @Override
    protected boolean isResourceServiceRestartAllowed(OperationContext context, ServiceController<?> service) {
        return true;
    }
}
