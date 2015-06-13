/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2014, Red Hat, Inc., and individual contributors
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

import java.io.Serializable;

import org.infinispan.server.jgroups.subsystem.JGroupsExtension;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.RunningMode;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.extension.ExtensionRegistry;
import org.jboss.as.controller.extension.ExtensionRegistryType;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.registry.Resource;
import org.jboss.as.domain.management.CoreManagementResourceDefinition;
import org.jboss.as.domain.management.audit.AccessAuditResourceDefinition;
import org.jboss.as.jmx.JMXExtension;
import org.jboss.as.subsystem.test.AdditionalInitialization;
import org.jboss.as.txn.subsystem.TransactionExtension;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.FILE_HANDLER;


/**
 * Initializer for subsystem dependencies
 *
 * @author Paul Ferraro
 * @author William Burns
 * @author Tristan Tarrant
 */
public class InfinispanSubsystemDependenciesInitialization extends AdditionalInitialization implements Serializable {
    private static final long serialVersionUID = -4433079373360352449L;
    private final RunningMode mode;

    public InfinispanSubsystemDependenciesInitialization() {
        this(RunningMode.ADMIN_ONLY);
    }

    public InfinispanSubsystemDependenciesInitialization(RunningMode mode) {
        this.mode = mode;
    }

    @Override
    protected RunningMode getRunningMode() {
        return this.mode;
    }

    @Override
    protected void initializeExtraSubystemsAndModel(ExtensionRegistry registry, Resource root,
            ManagementResourceRegistration registration) {
        initializeJGroupsSubsystem(registry, root, registration);
        initializeTxnSubsystem(registry, root, registration);
    }

    private void initializeJGroupsSubsystem(ExtensionRegistry registry, Resource root,
            ManagementResourceRegistration registration) {
        new JGroupsExtension()
                .initialize(registry.getExtensionContext("jgroups", registration, ExtensionRegistryType.MASTER));

        Resource subsystem = Resource.Factory.create();
        subsystem.getModel().get("default-stack").set("tcp");
        root.registerChild(
                PathElement.pathElement(ModelDescriptionConstants.SUBSYSTEM, JGroupsExtension.SUBSYSTEM_NAME),
                subsystem);

        Resource stack = Resource.Factory.create();
        subsystem.registerChild(PathElement.pathElement("stack", "tcp"), stack);

        Resource transport = Resource.Factory.create();
        transport.getModel().get("type").set("TCP");
        stack.registerChild(PathElement.pathElement(ModelKeys.TRANSPORT, ModelKeys.TRANSPORT_NAME), transport);
    }

    private void initializeTxnSubsystem(ExtensionRegistry registry, Resource root,
            ManagementResourceRegistration registration) {
        new TransactionExtension()
                .initialize(registry.getExtensionContext("transactions", registration, ExtensionRegistryType.MASTER));
    }
}