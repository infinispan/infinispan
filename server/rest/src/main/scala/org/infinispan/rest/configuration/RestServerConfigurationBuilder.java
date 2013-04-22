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
package org.infinispan.rest.configuration;

import org.infinispan.configuration.Builder;

/**
 * RestServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class RestServerConfigurationBuilder implements Builder<RestServerConfiguration> {

   private ExtendedHeaders extendedHeaders = ExtendedHeaders.ON_DEMAND;

   public RestServerConfigurationBuilder extendedHeaders(ExtendedHeaders extendedHeaders) {
      this.extendedHeaders = extendedHeaders;
      return this;
   }

   @Override
   public void validate() {
      // Nothing to do
   }

   @Override
   public RestServerConfiguration create() {
      return new RestServerConfiguration(extendedHeaders);
   }

   @Override
   public Builder<?> read(RestServerConfiguration template) {
      this.extendedHeaders = template.extendedHeaders();
      return this;
   }

   public RestServerConfiguration build() {
      return build(true);
   }

   public RestServerConfiguration build(boolean validate) {
      if (validate)
         validate();
      return create();
   }

}
