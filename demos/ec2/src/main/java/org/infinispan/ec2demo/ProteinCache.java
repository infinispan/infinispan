/**
 * 
 */
package org.infinispan.ec2demo;

import java.io.IOException;
import org.apache.log4j.*;
import org.infinispan.Cache;

/**
 * @author noelo
 * 
 */
public class ProteinCache {
	Logger myLogger = Logger.getLogger(ProteinCache.class);
	private Cache<String, Nucleotide_Protein_Element> myCache;

	public ProteinCache(CacheBuilder cacheManger) throws IOException {
		myCache = cacheManger.getCacheManager().getCache("ProteinCache");
	}

	public void addToCache(Nucleotide_Protein_Element value) {
		if (value == null)
			return;
		String myKey = value.getGenbankAccessionNumber();
		if ((myKey == null) || (myKey.isEmpty())) {
			myLogger.error("Invalid record " + value);
		} else {
			myCache.put(value.getGenbankAccessionNumber(), value);
		}
	}

	public int getCacheSize() {
		return myCache.size();
	}

	public Nucleotide_Protein_Element getProteinDetails(String GBAN) {
		Nucleotide_Protein_Element myVR = myCache.get(GBAN);
		return myVR;
	}

	public Cache getCache() {
		return myCache;
	}

}
