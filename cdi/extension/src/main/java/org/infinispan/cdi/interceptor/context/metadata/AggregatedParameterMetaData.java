/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
package org.infinispan.cdi.interceptor.context.metadata;

import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * Contains all parameters metadata for a method annotated with a cache annotation.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class AggregatedParameterMetaData {

   private final List<ParameterMetaData> parameters;
   private final List<ParameterMetaData> keyParameters;
   private final ParameterMetaData valueParameter;

   public AggregatedParameterMetaData(List<ParameterMetaData> parameters,
                                      List<ParameterMetaData> keyParameters,
                                      ParameterMetaData valueParameter) {

      this.parameters = unmodifiableList(parameters);
      this.keyParameters = unmodifiableList(keyParameters);
      this.valueParameter = valueParameter;
   }

   public List<ParameterMetaData> getParameters() {
      return parameters;
   }

   public List<ParameterMetaData> getKeyParameters() {
      return keyParameters;
   }

   public ParameterMetaData getValueParameter() {
      return valueParameter;
   }

   @Override
   public String toString() {
      return new StringBuilder()
            .append("AggregatedParameterMetaData{")
            .append("parameters=").append(parameters)
            .append(", keyParameters=").append(keyParameters)
            .append(", valueParameter=").append(valueParameter)
            .append('}')
            .toString();
   }
}
