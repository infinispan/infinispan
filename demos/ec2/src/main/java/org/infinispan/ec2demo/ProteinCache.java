package org.infinispan.ec2demo;

import org.infinispan.Cache;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;

/**
 * @author noelo
 * 
 */
public class ProteinCache {
	private static final Log log = LogFactory.getLog(ProteinCache.class);
	private Cache<String, Nucleotide_Protein_Element> myCache;

	public ProteinCache(CacheBuilder cacheManger) throws IOException {
		myCache = cacheManger.getCacheManager().getCache("ProteinCache");
	}

	public void addToCache(Nucleotide_Protein_Element value) {
		if (value == null)
			return;
		String myKey = value.getGenbankAccessionNumber();
		if ((myKey == null) || (myKey.isEmpty())) {
			log.error("Invalid record " + value);
		} else {
			myCache.put(value.getGenbankAccessionNumber(), value);
		}
	}

	public int getCacheSize() {
		return myCache.size();
	}

	public Nucleotide_Protein_Element getProteinDetails(String GBAN) {
      return myCache.get(GBAN);
	}

	public Cache<String, Nucleotide_Protein_Element> getCache() {
		return myCache;
	}

}
