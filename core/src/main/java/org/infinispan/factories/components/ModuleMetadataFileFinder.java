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

package org.infinispan.factories.components;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;

/**
 * This interface should be implemented by all Infinispan modules that expect to have components using {@link Inject},
 * {@link Start} or {@link Stop} annotations.  The metadata file is generated at build time and packaged in the module's
 * corresponding jar file (see Infinispan's <pre>core</pre> module <pre>pom.xml</pre> for an example of this).
 * <p />
 * Module component metadata is usually generated in a file titled <pre>${module-name}-component-metadata.dat</pre> and
 * typically resides in the root of the module's jar file.
 * <p />
 * For example, Infinispan's Query Module would implement this interface to return <pre>infinispan-query-component-metadata.dat</pre>.
 * <p />
 * Implementations of this interface are discovered using the JDK's {@link java.util.ServiceLoader} utility.  Which means
 * modules would also have to package a file called <pre>org.infinispan.factories.components.ModuleMetadataFileFinder</pre>
 * in the <pre>META-INF/services/</pre> folder in their jar, and this file would contain the fully qualified class name
 * of the module's implementation of this interface.
 * <p />
 * Please see Infinispan's query module for an example of this.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public interface ModuleMetadataFileFinder {
   String getMetadataFilename();
}
