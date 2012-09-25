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
package org.infinispan.configuration;

import org.infinispan.config.ConfigurationException;

/**
 * ConfigurationUtils. Contains utility methods used in configuration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public final class ConfigurationUtils {

   private ConfigurationUtils() {}

   @SuppressWarnings("unchecked")
   public static <B> Class<? extends Builder<B>> builderFor(B built) {
      BuiltBy builtBy = built.getClass().getAnnotation(BuiltBy.class);
      if (builtBy == null) {
         throw new ConfigurationException("Missing BuiltBy annotation for configuration bean " + built.getClass().getName());
      }
      return (Class<? extends Builder<B>>) builtBy.value();
   }
}
