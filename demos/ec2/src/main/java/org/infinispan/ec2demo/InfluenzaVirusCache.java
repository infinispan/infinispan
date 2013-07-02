package org.infinispan.ec2demo;

import org.infinispan.Cache;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;

/**
 * @author noconnor@redhat.com
 * 
 */
public class InfluenzaVirusCache {
	private static final Log log = LogFactory.getLog(InfluenzaVirusCache.class);
	private Cache<String, Influenza_N_P_CR_Element> myCache;

	public InfluenzaVirusCache(CacheBuilder cacheManger) throws IOException {
		myCache = cacheManger.getCacheManager().getCache("InfluenzaCache");
	}

	public void addToCache(Influenza_N_P_CR_Element value) {
		if (value == null)
			return;
		String myKey = value.getGanNucleoid();
		if ((myKey == null) || (myKey.isEmpty())) {
			log.error("Invalid record " + value);
		} else {
			myCache.put(myKey, value);
		}
	}

	public int getCacheSize() {
		return myCache.size();
	}

	public Influenza_N_P_CR_Element getVirusDetails(String GBAN) {
      return myCache.get(GBAN);
	}
	
	public Cache<String, Influenza_N_P_CR_Element> getCache(){
		return myCache;
	}
}
