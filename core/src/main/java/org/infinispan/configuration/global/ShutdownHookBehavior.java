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
package org.infinispan.configuration.global;

/**
 * Behavior of the JVM shutdown hook registered by the cache
 */
public enum ShutdownHookBehavior {
   /**
    * By default a shutdown hook is registered if no MBean server (apart from the JDK default) is detected.
    */
   DEFAULT,
   /**
    * Forces the cache to register a shutdown hook even if an MBean server is detected.
    */
   REGISTER,
   /**
    * Forces the cache NOT to register a shutdown hook, even if no MBean server is detected.
    */
   DONT_REGISTER
}