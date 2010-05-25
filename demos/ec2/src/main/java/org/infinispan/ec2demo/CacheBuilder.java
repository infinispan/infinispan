/**
 *
 */
package org.infinispan.ec2demo;

import java.io.IOException;

import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author noconnor@redhat.com
 */
public class CacheBuilder {
	private EmbeddedCacheManager cache_manager;
	private static final Log myLogger = LogFactory.getLog(CacheBuilder.class);

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
	private CacheManager currCache;

	/**
	 * @param cache_manager
	 */
	public ShutdownHook(CacheManager cache_manager) {
		currCache = cache_manager;
	}

	public void run() {
		System.out.println("Shutting down Cache Manager");
		currCache.stop();
	}
}
