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
package org.infinispan.factories.annotations;

import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.ComponentRegistry;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Used to annotate a method as one that is used to inject a registered component into another component.  The component
 * to be constructed must be built using the {@link AbstractComponentFactory#construct(Class)} method, or if your object
 * that needs components injected into it already exists, it can be built using the {@link
 * ComponentRegistry#wireDependencies(Object)} method.
 * <p/>
 * Usage example:
 * <pre>
 *       public class MyClass
 *       {
 *          private TransactionManager tm;
 *          private BuddyManager bm;
 *          private Notifier n;
 * <p/>
 *          &amp;Inject
 *          public void setTransactionManager(TransactionManager tm)
 *          {
 *             this.tm = tm;
 *          }
 * <p/>
 *          &amp;Inject
 *          public void injectMoreStuff(BuddyManager bm, Notifier n)
 *          {
 *             this.bm = bm;
 *             this.n = n;
 *          }
 *       }
 * <p/>
 * </pre>
 * and an instance of this class can be constructed and wired using
 * <pre>
 *       MyClass myClass = componentFactory.construct(MyClass.class); // instance will have dependencies injected.
 * </pre>
 * 
 * Methods annotated with this Inject annotation should *only* set class fields. They should do nothing else.
 * If you need to do some work to prepare the component for use, do it in a {@link @Start} method since this is only 
 * called once when a component starts.  
 *
 * @author Manik Surtani
 * @since 4.0
 */
// ensure this annotation is available at runtime.
@Retention(RetentionPolicy.RUNTIME)

// only applies to fields.
@Target(ElementType.METHOD)
public @interface Inject {
}