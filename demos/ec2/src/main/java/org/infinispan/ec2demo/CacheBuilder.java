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
package org.infinispan.ec2demo;

import java.io.IOException;

import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * @author noconnor@redhat.com
 */
public class CacheBuilder {
	private EmbeddedCacheManager cache_manager;

	public CacheBuilder(String inConfigFile) throws IOException {
		
		if ((inConfigFile==null)||(inConfigFile.isEmpty()))
			throw new RuntimeException(
					"Infinispan configuration file not found-->"+inConfigFile);

		System.out.println("CacheBuilder called with "+inConfigFile);
		
		cache_manager = new DefaultCacheManager(inConfigFile, false);
		//ShutdownHook shutdownHook = new ShutdownHook(cache_manager);
		//Runtime.getRuntime().addShutdownHook(shutdownHook);
	}

	public EmbeddedCacheManager getCacheManager() {
		return this.cache_manager;
	}

}

class ShutdownHook extends Thread {
	private CacheContainer currCache;

	/**
	 * @param cache_container
	 */
	public ShutdownHook(CacheContainer cache_container) {
		currCache = cache_container;
	}

	@Override
   public void run() {
		System.out.println("Shutting down Cache Manager");
		currCache.stop();
	}
}
