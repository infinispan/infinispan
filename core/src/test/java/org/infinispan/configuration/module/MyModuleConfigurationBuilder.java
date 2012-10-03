/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.configuration.module;

import org.infinispan.configuration.Builder;
import org.infinispan.configuration.cache.AbstractModuleConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;

/**
 *
 * MyModuleConfigurationBuilder. A builder for {@link MyModuleConfiguration}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class MyModuleConfigurationBuilder extends AbstractModuleConfigurationBuilder implements Builder<MyModuleConfiguration> {
   private String attribute;

   public MyModuleConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   public MyModuleConfigurationBuilder attribute(String attribute) {
      this.attribute = attribute;
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public MyModuleConfiguration create() {
      return new MyModuleConfiguration(attribute);
   }

   @Override
   public MyModuleConfigurationBuilder read(MyModuleConfiguration template) {
      this.attribute(template.attribute());
      return this;
   }
}
