/**
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
 *   ~
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
 * <h1>Spring Infinispan - Shared classes.</h1>
 * <p>
 * This package contains classes that are shared between the two major themes underlying <em>Spring Infinispan</em>: 
 * <ol>
 *   <li>
 *     Implement a provider for <a href="http://www.springsource.com">Spring</a> 3.1's Cache abstraction backed by the open-source 
 *     high-performance distributed cache <a href="http://www.jboss.org/infinispan">JBoss Infinispan</a>.<br/><br/>
 *     See package {@link org.infinispan.spring.spi <code>org.infinispan.spring.spi</code>}.<br/><br/>
 *   </li>
 *   <li>
 *     Provide implementations of Spring's {@link org.springframework.beans.factory.FactoryBean <code>FactoryBean</code>}
 *     interface for easing usage of JBoss Infinispan within the Spring programming model.<br/><br/>
 *     See package {@link org.infinispan.spring.support <code>org.infinispan.spring.support</code>}.
 *   </li>
 * </ol>
 * </p>
 */
package org.infinispan.spring;

