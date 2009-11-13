/**
 *
 */
package org.infinispan.ec2demo;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import java.io.IOException;
import java.util.Properties;

/**
 * @author noconnor@redhat.com
 */
public class CacheBuilder {
	private CacheManager cache_manager;
	private static final Log myLogger = LogFactory.getLog(CacheBuilder.class);

	public CacheBuilder(String inConfigFile) throws IOException {
		//system property gets priority
		String configFile = System.getProperty("EC2Demo-jgroups-config");		

		if ((configFile==null)||(configFile.isEmpty()))
			configFile = inConfigFile;
		
		if ((configFile==null)||(configFile.isEmpty()))
			throw new RuntimeException(
					"Need to either set system property EC2Demo-jgroups-config to point to the jgroups configuration file or pass in the the location of the jgroups configuration file");

		System.out.println("CacheBuilder called with "+configFile);
		
		GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();
		gc.setClusterName("infinispan-demo-cluster");
		gc.setTransportClass(JGroupsTransport.class.getName());
		Properties p = new Properties();
		p.setProperty("configurationFile", configFile);
		gc.setTransportProperties(p);

		Configuration c = new Configuration();
		c.setCacheMode(Configuration.CacheMode.DIST_SYNC);
		c.setExposeJmxStatistics(true);
		c.setUnsafeUnreliableReturnValues(true);
		c.setNumOwners(3);
		c.setL1CacheEnabled(true);
		c.setInvocationBatchingEnabled(true);
		c.setUseReplQueue(true);
		c.setL1Lifespan(6000000);
		cache_manager = new DefaultCacheManager(gc, c, false);
		//ShutdownHook shutdownHook = new ShutdownHook(cache_manager);
		//Runtime.getRuntime().addShutdownHook(shutdownHook);
	}

	public CacheManager getCacheManager() {
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
