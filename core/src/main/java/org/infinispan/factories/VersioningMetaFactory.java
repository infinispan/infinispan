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

package org.infinispan.factories;

import org.infinispan.container.versioning.NumericVersionGenerator;
import org.infinispan.container.versioning.SimpleClusteredVersionGenerator;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.util.AnyEquivalence;
import org.infinispan.util.Equivalence;

/**
 * Version generator component factory. Version generators are used for
 * situations where version or ids are needed, e.g. data versioning,
 * transaction recovery, or hotrod/memcached support.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 5.1
 */
@DefaultFactoryFor(classes = VersionGenerator.class)
@SuppressWarnings("unused")
public class VersioningMetaFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @SuppressWarnings("unchecked")
   @Override
   public <T> T construct(Class<T> componentType) {
      // TODO: Eventually, NumericVersionGenerator and SimpleClusteredVersionGenerator should be merged into one...
      switch (configuration.versioning().scheme()) {
         case SIMPLE: {
            if (configuration.clustering().cacheMode().isClustered())
               return (T) new SimpleClusteredVersionGenerator();

            return (T) new NumericVersionGenerator();
         }
         default:
            return (T) new NumericVersionGenerator();
      }
   }

}
