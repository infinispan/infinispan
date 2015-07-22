package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.ResourceDefinition;

/**
 * RestartableResource.
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public interface RestartableResourceDefinition extends ResourceDefinition {
    RestartableResourceServiceInstaller getServiceInstaller();

    boolean isRuntimeRegistration();
}
