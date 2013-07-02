package org.infinispan.ec2demo;

import java.io.IOException;
import org.apache.log4j.*;
import org.infinispan.Cache;

/**
 * @author noconnor@redhat.com
 * 
 */
public class NucleotideCache {
	private static final Logger log = Logger.getLogger(NucleotideCache.class);
	private Cache<String, Nucleotide_Protein_Element> myCache;

	public NucleotideCache(CacheBuilder cacheManger) throws IOException {
		myCache = cacheManger.getCacheManager().getCache("NucleotideCache");
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

	public Nucleotide_Protein_Element getNucleotideDetails(String GBAN) {
      return myCache.get(GBAN);
	}
	
	public Cache<String, Nucleotide_Protein_Element> getCache(){
		return myCache;
	}
}
