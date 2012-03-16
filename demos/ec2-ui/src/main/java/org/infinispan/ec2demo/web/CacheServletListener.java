/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.ec2demo.web;

import org.infinispan.ec2demo.CacheBuilder;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;

/**
 * @author noconnor@redhat.com
 *
 */
public class CacheServletListener implements ServletContextListener {
	private CacheBuilder currCacheBuilder;
	private ServletContext context = null;

	/* (non-Javadoc)
	 * @see javax.servlet.ServletContextListener#contextDestroyed(javax.servlet.ServletContextEvent)
	 */
	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		currCacheBuilder.getCacheManager().stop();
	}

	/* (non-Javadoc)
	 * @see javax.servlet.ServletContextListener#contextInitialized(javax.servlet.ServletContextEvent)
	 */
	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		System.out.println("in CacheServletListener");
		try {
			this.context = arg0.getServletContext();
			String x = arg0.getServletContext().getInitParameter("InfinispanConfigFile");
			currCacheBuilder =  new CacheBuilder(x);
			System.out.println("in CacheServletListener...starting cache");
			currCacheBuilder.getCacheManager().start();
			System.out.println("in CacheServletListener...starting cache...done");
			context.setAttribute("cacheBuilder", currCacheBuilder);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("in CacheServletListener...exit");
	}

}
