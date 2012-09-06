/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.factories;

import org.infinispan.config.ConfigurationException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.statetransfer.*;

/**
 * Constructs {@link org.infinispan.statetransfer.StateTransferManager},
 * {@link org.infinispan.statetransfer.StateConsumer}
 * and {@link org.infinispan.statetransfer.StateProvider} instances.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @author anistor@redhat.com
 * @since 4.0
 */
@DefaultFactoryFor(classes = {StateTransferManager.class, StateConsumer.class, StateProvider.class})
public class StateTransferComponentFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public <T> T construct(Class<T> componentType) {
      if (!configuration.clustering().cacheMode().isClustered())
         return null;

      if (componentType.equals(StateTransferManager.class)) {
         return componentType.cast(new StateTransferManagerImpl());
      } else if (componentType.equals(StateProvider.class)) {
         return componentType.cast(new StateProviderImpl());
      } else if (componentType.equals(StateConsumer.class)) {
         return componentType.cast(new StateConsumerImpl());
      }

      throw new ConfigurationException("Don't know how to create a " + componentType.getName());
   }
}
