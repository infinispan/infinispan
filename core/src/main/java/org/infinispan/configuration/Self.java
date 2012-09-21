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

import org.infinispan.configuration.cache.LoaderConfigurationBuilder;

/**
 * This interface simplifies the task of writing fluent builders which need to inherit from
 * other builders (abstract or concrete). It overcomes Java's limitation of not being able to
 * return an instance of a class narrowed to the class itself. It should be used by all {@link Builder}
 * classes which require inheritance (such as the {@link LoaderConfigurationBuilder})
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface Self<S extends Self<S>> {
   S self();
}
