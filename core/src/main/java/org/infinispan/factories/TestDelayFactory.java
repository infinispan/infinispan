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

package org.infinispan.factories;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Internal class, only used for testing.
 *
 * It has to reside in the production source tree because the component metadata persister doesn't parse
 * test classes.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
@DefaultFactoryFor(classes = TestDelayFactory.Component.class)
public class TestDelayFactory extends AbstractComponentFactory implements AutoInstantiableFactory {
   private boolean injectionDone = false;
   private Control control;

   @Inject
   public void inject(Control control) throws InterruptedException {
      this.control = control;
      control.await();
      injectionDone = true;
   }

   public <T> T construct(Class<T> componentType) {
      if (!injectionDone) {
         throw new IllegalStateException("GlobalConfiguration reference is null");
      }
      return componentType.cast(new Component());
   }

   @Scope(Scopes.NAMED_CACHE)
   public static class Component {
   }

   @Scope(Scopes.GLOBAL)
   public static class Control {
      private final CountDownLatch latch = new CountDownLatch(1);

      public void await() throws InterruptedException {
         latch.await(10, TimeUnit.SECONDS);
      }

      public void unblock() {
         latch.countDown();
      }
   }
}
