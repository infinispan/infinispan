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
package org.infinispan.cdi.util;

import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Collections.addAll;

/**
 * An helper class providing useful methods to work with JDK collections.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public final class CollectionsHelper {
   /**
    * Disable instantiation.
    */
   private CollectionsHelper() {
   }

   /**
    * Creates a {@link Set} with the given elements.
    *
    * @param elements the elements.
    * @param <T>      the element type.
    * @return a new {@link Set} instance containing the given elements.
    * @throws NullPointerException if parameter elements is {@code null}.
    */
   public static <T> Set<T> asSet(T... elements) {
      final Set<T> resultSet = new LinkedHashSet<T>();
      addAll(resultSet, elements);

      return resultSet;
   }
}
