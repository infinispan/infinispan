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

import java.util.Locale;
import java.util.ResourceBundle;

import org.jboss.as.controller.descriptions.StandardResourceDescriptionResolver;

public class SharedResourceDescriptionResolver extends StandardResourceDescriptionResolver {
   final String commonPrefix;

   public SharedResourceDescriptionResolver(String keyPrefix, String bundleBaseName, ClassLoader bundleLoader, boolean reuseAttributesForAdd, boolean useUnprefixedChildTypes, String commonPrefix) {
      super(keyPrefix, bundleBaseName, bundleLoader, reuseAttributesForAdd, useUnprefixedChildTypes);
      this.commonPrefix = commonPrefix;
   }

   @Override
   public String getResourceAttributeDescription(String attributeName, Locale locale, ResourceBundle bundle) {
      return super.getResourceAttributeDescription(attributeName, locale, bundle);
   }

   @Override
   public String getResourceAttributeValueTypeDescription(String attributeName, Locale locale, ResourceBundle bundle, String... suffixes) {
      return super.getResourceAttributeValueTypeDescription(attributeName, locale, bundle, suffixes);
   }

   @Override
   public String getOperationParameterDescription(String operationName, String paramName, Locale locale, ResourceBundle bundle) {
      return super.getOperationParameterDescription(operationName, paramName, locale, bundle);
   }

   @Override
   public String getOperationParameterValueTypeDescription(String operationName, String paramName, Locale locale, ResourceBundle bundle, String... suffixes) {
      return super.getOperationParameterValueTypeDescription(operationName, paramName, locale, bundle, suffixes);
   }

   @Override
   public String getChildTypeDescription(String childType, Locale locale, ResourceBundle bundle) {
      return super.getChildTypeDescription(childType, locale, bundle);
   }

   private String getBundleKey(String... args) {
      return getVariableBundleKey(args);
   }

   private String getVariableBundleKey(String[] fixed, String... variable) {
      StringBuilder sb = new StringBuilder(getKeyPrefix());
      for (String arg : fixed) {
         if (sb.length() > 0) {
            sb.append('.');
         }
         sb.append(arg);
      }
      if (variable != null) {
         for (String arg : variable) {
            if (sb.length() > 0) {
               sb.append('.');
            }
            sb.append(arg);
         }
      }
      return sb.toString();
   }

}
