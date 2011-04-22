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

/**
 * Commands that operate on the cache, either locally or remotely.  This package contains the entire command object
 * model including interfaces and abstract classes.  Your starting point is probably {@link ReplicableCommand}, which
 * represents a command that can be used in RPC calls.
 * <p />
 * A sub-interface, {@link VisitableCommand}, represents commands that can be visited using the <a href="http://en.wikipedia.org/wiki/Visitor_pattern">visitor pattern</a>.
 * Most commands that relate to public {@link Cache} API methods tend to be {@link VisitableCommand}s, and hence the
 * importance of this interface.
 * <p />
 * The {@link Visitor} interface is capable of visiting {@link VisitableCommand}s, and a useful abstract implementation
 * of {@link Visitor} is {@link org.infinispan.interceptors.base.CommandInterceptor}, which allows you to create
 * interceptors that intercept command invocations adding aspects of behavior to a given invocation.
 *
 * @author Manik Surtani
 * @since 4.0
 */
package org.infinispan.commands;