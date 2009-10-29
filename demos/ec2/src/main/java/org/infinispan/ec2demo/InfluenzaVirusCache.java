/**
 * 
 */
package org.infinispan.ec2demo;

import java.io.IOException;
import java.net.URL;
import org.apache.log4j.Logger;
import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;

/**
 * @author noconnor@redhat.com
 * 
 */
public class InfluenzaVirusCache {
	Logger myLogger = Logger.getLogger(InfluenzaVirusCache.class);
	private Cache<String, Influenza_N_P_CR_Element> myCache;

	public InfluenzaVirusCache(CacheBuilder cacheManger) throws IOException {
		myCache = cacheManger.getCacheManager().getCache("InfluenzaCache");
	}

	public void addToCache(Influenza_N_P_CR_Element value) {
		if (value == null)
			return;
		String myKey = value.getGanNucleoid();
		if ((myKey == null) || (myKey.isEmpty())) {
			myLogger.error("Invalid record " + value);
		} else {
			myCache.put(myKey, value);
		}
	}

	public int getCacheSize() {
		return myCache.size();
	}

	public Influenza_N_P_CR_Element getVirusDetails(String GBAN) {
		Influenza_N_P_CR_Element myVR = myCache.get(GBAN);
		return myVR;
	}
	
	public Cache getCache(){
		return myCache;
	}
}
