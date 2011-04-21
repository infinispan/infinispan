/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.marshall;

import java.util.Set;

/**
 * {@link AdvancedExternalizer} provides an alternative way to provide
 * externalizers for marshalling/unmarshalling user defined classes that
 * overcome the deficiencies of the more user-friendly externalizer definition
 * model explained in {@link Externalizer}.
 *
 * The first noticeable difference is that this method does not require user
 * classes to be annotated in anyway, so it can be used with classes for which
 * source code is not available or that cannot be modified. The bound between
 * the externalizer and the classes that are marshalled/unmarshalled is set by
 * providing an implementation for {@link #getTypeClasses()} which should
 * return the list of classes that this externalizer can marshall.
 *
 * Secondly, in order to save the maximum amount of space possible in the
 * payloads generated, this externalizer method requires externalizer
 * implementations to provide a positive identified via {@link #getId()}
 * implementations or via XML/programmatic configuration that identifies the
 * externalizer when unmarshalling a payload.  In order for this to work
 * however, this externalizer method requires externalizers to be registered
 * on cache manager creation time via XML or programmatic configuration. On
 * the contrary, externalizers based on {@link Externalizer} and
 * {@link SerializeWith} require no pre-registration whatsoever.
 *
 * Internally, Infinispan uses this advanced externalizer mechanism in order
 * to marshall/unmarshall internal classes.
 *
 * Finally, {@link AbstractExternalizer} provides default implementations for
 * some of the methods defined in this interface and so it's generally
 * recommended that implementations extend that abstract class instead of
 * implementing {@link AdvancedExternalizer} directly.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public interface AdvancedExternalizer<T> extends Externalizer<T> {
   
   /**
    * Returns a collection of Class instances representing the types that this
    * AdvancedExternalizer can marshall.  Clearly, empty sets are not allowed.
    * The externalizer framework currently requires all individual types to be
    * listed since it does not make assumptions based on super classes or
    * interfaces.
    *
    * @return A set containing the Class instances that can be marshalled.
    */
   Set<Class<? extends T>> getTypeClasses();

   /**
    * Returns an integer that identifies the externalizer type. This is used
    * at read time to figure out which {@link AdvancedExternalizer} should read
    * the contents of the incoming buffer.
    *
    * Using a positive integer allows for very efficient variable length
    * encoding of numbers, and it's much more efficient than shipping
    * {@link AdvancedExternalizer} implementation class information around.
    * Negative values are not allowed.
    *
    * Implementers of this interface can use any positive integer as long as
    * it does not clash with any other identifier in the system.  You can find
    * information on the pre-assigned identifier ranges in
    * <a href="http://community.jboss.org/docs/DOC-16198">here</a>.
    *
    * It's highly recommended that maintaining of these identifiers is done
    * in a centralized way and you can do so by making annotations reference
    * a set of statically defined identifiers in a separate class or
    * interface.  Such class/interface gives a global view of the identifiers
    * in use and so can make it easier to assign new ids.
    *
    * Implementors can optionally avoid giving a meaningful implementation to
    * this method (i.e. return null) and instead rely on XML or programmatic
    * configuration to provide the AdvancedExternalizer id.  If no id can be
    * determined via the implementation or XML/programmatic configuration, an
    * error will be reported.  If an id has been defined both via the
    * implementation and XML/programmatic configuration, the value defined via
    * XML/programmatic configuration will be used ignoring the other.
    *
    * @return A positive identifier for the AdvancedExternalizer.
    */
   Integer getId();

}
