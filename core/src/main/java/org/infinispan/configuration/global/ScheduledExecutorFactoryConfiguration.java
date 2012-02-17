/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.global;

import org.infinispan.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.util.TypedProperties;

public class ScheduledExecutorFactoryConfiguration extends AbstractTypedPropertiesConfiguration {

   private final ScheduledExecutorFactory factory;

   ScheduledExecutorFactoryConfiguration(ScheduledExecutorFactory factory, TypedProperties properties) {
      super(properties);
      this.factory = factory;
   }

   public ScheduledExecutorFactory factory() {
      return factory;
   }

   @Override
   public String toString() {
      return "ScheduledExecutorFactoryConfiguration{" +
            "factory=" + factory +
            '}';
   }

}