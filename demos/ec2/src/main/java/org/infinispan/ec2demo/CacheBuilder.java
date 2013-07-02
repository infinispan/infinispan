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
