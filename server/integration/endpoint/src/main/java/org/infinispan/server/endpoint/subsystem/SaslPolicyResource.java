/*
* JBoss, Home of Professional Open Source.
* Copyright 2011, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.server.endpoint.subsystem;

import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * SaslPolicyResource.
 *
 * @author <a href="kabir.khan@jboss.com">Kabir Khan</a>
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SaslPolicyResource extends SimpleResourceDefinition {
    static final PathElement SASL_POLICY_PATH = PathElement.pathElement(ModelKeys.SASL_POLICY, ModelKeys.SASL_POLICY_NAME);

    static final SimpleAttributeDefinition FORWARD_SECRECY = createBooleanAttributeDefinition(ModelKeys.FORWARD_SECRECY);
    static final SimpleAttributeDefinition NO_ACTIVE = createBooleanAttributeDefinition(ModelKeys.NO_ACTIVE);
    static final SimpleAttributeDefinition NO_ANONYMOUS = createBooleanAttributeDefinition(ModelKeys.NO_ANONYMOUS);
    static final SimpleAttributeDefinition NO_DICTIONARY = createBooleanAttributeDefinition(ModelKeys.NO_DICTIONARY);
    static final SimpleAttributeDefinition NO_PLAIN_TEXT = createBooleanAttributeDefinition(ModelKeys.NO_PLAIN_TEXT);
    static final SimpleAttributeDefinition PASS_CREDENTIALS = createBooleanAttributeDefinition(ModelKeys.PASS_CREDENTIALS);


    static final SimpleAttributeDefinition[] ATTRIBUTES = {
            FORWARD_SECRECY,
            NO_ACTIVE,
            NO_ANONYMOUS,
            NO_DICTIONARY,
            NO_PLAIN_TEXT,
            PASS_CREDENTIALS,

    };

    SaslPolicyResource() {
        super(SASL_POLICY_PATH,
                EndpointExtension.getResourceDescriptionResolver(ModelKeys.SASL_POLICY),
                SaslPolicyAdd.INSTANCE,
                ReloadRequiredRemoveStepHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        final OperationStepHandler writeHandler =
                new ReloadRequiredWriteAttributeHandler(FORWARD_SECRECY, NO_ACTIVE, NO_ANONYMOUS, NO_DICTIONARY,
                        NO_PLAIN_TEXT, PASS_CREDENTIALS);
        resourceRegistration.registerReadWriteAttribute(FORWARD_SECRECY, null, writeHandler);
        resourceRegistration.registerReadWriteAttribute(NO_ACTIVE, null, writeHandler);
        resourceRegistration.registerReadWriteAttribute(NO_ANONYMOUS, null, writeHandler);
        resourceRegistration.registerReadWriteAttribute(NO_DICTIONARY, null, writeHandler);
        resourceRegistration.registerReadWriteAttribute(NO_PLAIN_TEXT, null, writeHandler);
        resourceRegistration.registerReadWriteAttribute(PASS_CREDENTIALS, null, writeHandler);
    }

    private static SimpleAttributeDefinition createBooleanAttributeDefinition(String name) {
        return SimpleAttributeDefinitionBuilder.create(name, ModelType.BOOLEAN)
                .setDefaultValue(new ModelNode(true))
                .setAllowNull(true)
                .setAllowExpression(true)
                .setAttributeMarshaller(new WrappedAttributeMarshaller(Attribute.VALUE))
                .build();
    }
}
