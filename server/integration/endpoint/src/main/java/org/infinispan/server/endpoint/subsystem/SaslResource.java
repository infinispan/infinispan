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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.ListAttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.StringListAttributeDefinition;
import org.jboss.as.controller.operations.validation.AllowedValuesValidator;
import org.jboss.as.controller.operations.validation.StringLengthValidator;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * SaslResource.
 *
 * @author <a href="kabir.khan@jboss.com">Kabir Khan</a>
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SaslResource extends SimpleResourceDefinition {
   private static final PathElement SASL_PATH = PathElement.pathElement(ModelKeys.SASL, ModelKeys.SASL_NAME);

   static final ListAttributeDefinition MECHANISMS =
         new StringListAttributeDefinition.Builder(ModelKeys.MECHANISMS)
            .build();
   static final ListAttributeDefinition QOP =
         new StringListAttributeDefinition.Builder(ModelKeys.QOP)
            .setAllowNull(true)
            .setValidator(QopParameterValidation.INSTANCE)
            .build();
   static final SimpleAttributeDefinition SERVER_CONTEXT_NAME =
         new SimpleAttributeDefinitionBuilder(ModelKeys.SERVER_CONTEXT_NAME, ModelType.STRING, true)
            .setAllowExpression(true)
            .setXmlName(ModelKeys.SERVER_CONTEXT_NAME)
            .setRestartAllServices()
            .build();
   static final SimpleAttributeDefinition SERVER_NAME =
         new SimpleAttributeDefinitionBuilder(ModelKeys.SERVER_NAME, ModelType.STRING, true)
            .setAllowExpression(true)
            .setXmlName(ModelKeys.SERVER_NAME)
            .setRestartAllServices()
            .build();
   static final ListAttributeDefinition STRENGTH =
         new StringListAttributeDefinition.Builder(ModelKeys.STRENGTH)
            .setAllowNull(true)
            .setValidator(StrengthParameterValidation.INSTANCE)
            .build();

   static final AttributeDefinition[] SASL_ATTRIBUTES = { MECHANISMS, QOP, SERVER_CONTEXT_NAME, SERVER_NAME, STRENGTH };

   SaslResource() {
      super(SASL_PATH, EndpointExtension.getResourceDescriptionResolver(ModelKeys.SASL), SaslAdd.INSTANCE, ReloadRequiredRemoveStepHandler.INSTANCE);
   }

   @Override
   public void registerChildren(ManagementResourceRegistration resourceRegistration) {
      resourceRegistration.registerSubModel(new SaslPolicyResource());
      resourceRegistration.registerSubModel(new SaslPropertyResource());
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
      final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(SASL_ATTRIBUTES);
      for (AttributeDefinition attr : SASL_ATTRIBUTES) {
          resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
      }
   }

   private abstract static class SaslEnumValidator extends StringLengthValidator implements AllowedValuesValidator {
      final List<ModelNode> allowedValues = new ArrayList<ModelNode>();

      SaslEnumValidator(Enum<?>[] src, boolean toLowerCase) {
         super(1);
         for (Enum<?> e : src) {
            allowedValues.add(new ModelNode().set(toLowerCase ? e.name().toLowerCase(Locale.ENGLISH) : e.name()));
         }
      }

      @Override
      public List<ModelNode> getAllowedValues() {
         return allowedValues;
      }

   }

   private static class QopParameterValidation extends SaslEnumValidator implements AllowedValuesValidator {
      static final QopParameterValidation INSTANCE = new QopParameterValidation();

      public QopParameterValidation() {
         super(SaslQop.values(), false);
      }
   }

   private static class StrengthParameterValidation extends SaslEnumValidator implements AllowedValuesValidator {
      static final StrengthParameterValidation INSTANCE = new StrengthParameterValidation();

      public StrengthParameterValidation() {
         super(SaslStrength.values(), true);
      }
   }
}
