package org.jboss.as.clustering.infinispan.subsystem;

import java.util.Collection;

import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

/**
 * RestartableResourceAddHandler.
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public interface RestartableResourceServiceInstaller {

    Collection<ServiceController<?>> installRuntimeServices(OperationContext context, ModelNode operation, ModelNode containerModel, ModelNode cacheModel) throws OperationFailedException;

}
