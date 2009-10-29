/**
 * 
 */
package org.infinispan.ec2demo;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.*;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;

/**
 * @author noconnor@redhat.com
 * 
 */
public class CacheBuilder {
	Logger myLogger = Logger.getLogger(CacheBuilder.class);
	private CacheManager cache_manager;

	public CacheBuilder(String configFile) throws IOException {
		String jGroupsFile = System.getProperty("EC2Demo-jgroups-config");
		if (jGroupsFile==null)
			throw new RuntimeException("Need to set system property EC2Demo-jgroups-config to point to the jgroups configuration file");
		
		GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();
		gc.setClusterName("infinispan-demo-cluster");
		gc.setTransportClass(JGroupsTransport.class.getName());		
		Properties p = new Properties();
		p.setProperty("configurationFile",jGroupsFile);
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
		cache_manager = new DefaultCacheManager(gc, c,false);
        ShutdownHook shutdownHook = new ShutdownHook(cache_manager);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
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
