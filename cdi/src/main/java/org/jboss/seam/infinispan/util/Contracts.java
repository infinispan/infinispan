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
package org.jboss.seam.infinispan.util;

/**
 * An helper class providing useful assertion methods.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public final class Contracts {

   /**
    * Disable instantiation.
    */
   private Contracts() {
   }

   /**
    * Asserts that the given parameter cannot be null. If the parameter is {@code null} a {@link NullPointerException}
    * is thrown.
    *
    * @param parameter The parameter to check.
    * @param message   The exception message if parameter id {@code null}.
    */
   public static void assertNotNull(Object parameter, String message) {
      if (parameter == null) {
         throw new NullPointerException(message);
      }
   }
}
