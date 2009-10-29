/**
 * 
 */
package org.infinispan.ec2demo;

import java.io.IOException;
import java.util.List;
import org.infinispan.Cache;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferException;
import org.apache.log4j.*;
import org.xml.sax.SAXException;
import com.martiansoftware.jsap.JSAPResult;

/**
 * @author noconnor@redhat.com
 * 
 */
public class InfluenzaDataLoader {
	public CacheBuilder cbuilder;
	public Cache<String, Influenza_N_P_CR_Element> influenzaCache;
	public Cache<String, Nucleotide_Protein_Element> proteinCache;
	public Cache<String, Nucleotide_Protein_Element> nucleiodCache;
	private Nucleotide_Protein_Parser npParser;
	private Influenza_Parser iParser;

	Logger myLogger = Logger.getLogger(InfluenzaDataLoader.class);

	public void populateCache(JSAPResult config) throws SAXException {
		String cfgFileName = System.getProperty("infinispan.demo.cfg");
		if (cfgFileName == null)
			cfgFileName = config.getString("InfinispanCfg");

		try {
			cbuilder = new CacheBuilder(cfgFileName);
			influenzaCache = cbuilder.getCacheManager().getCache("InfluenzaCache");
			proteinCache = cbuilder.getCacheManager().getCache("ProteinCache");
			nucleiodCache = cbuilder.getCacheManager().getCache("NucleotideCache");
		
			npParser = new Nucleotide_Protein_Parser();
			iParser = new Influenza_Parser();

			System.out.println("Caches created....Starting CacheManager");
			cbuilder.getCacheManager().start();

			List<Address> z = cbuilder.getCacheManager().getMembers();
			for (Address k : z)
				if (k != null)
					System.out.println("Cache Address=" + k.toString());

			System.out.println("Parsing files....");
			
			if (config.getString("ifile") != null) {
				myLogger.info("Parsing Influenza data");
				List<Influenza_N_P_CR_Element> iList = iParser.parseFile(config.getString("ifile"));
				System.out.println("About to load " + iList.size() + " influenza elements into influenzaCache");
				int loopCount = 0;
				influenzaCache.startBatch();
				for (Influenza_N_P_CR_Element x : iList) {
					influenzaCache.put(x.getGanNucleoid(), x);
					loopCount++;
					if ((loopCount % 5000) == 0) {
						System.out.println("Added " + loopCount + " Influenza records");
						influenzaCache.endBatch(true);
						influenzaCache.startBatch();
					}
				}
				influenzaCache.endBatch(true);
				System.out.println("Loaded " + influenzaCache.size() + " influenza elements into influenzaCache");
			}

			if (config.getString("pfile") != null) {
				myLogger.info("Parsing Protein data");
				List<Nucleotide_Protein_Element> npList = npParser.parseFile(config.getString("pfile"));
				System.out.println("About to load " + npList.size() + " protein elements into ProteinCache");
				int loopCount = 0;
				proteinCache.startBatch();
				for (Nucleotide_Protein_Element x : npList) {
					proteinCache.put(x.getGenbankAccessionNumber(),x);
					loopCount++;
					if ((loopCount % 5000) == 0) {
						System.out.println("Added " + loopCount + " protein records");
						proteinCache.endBatch(true);
						proteinCache.startBatch();
					}
				}
				proteinCache.endBatch(true);
				System.out.println("Loaded " + proteinCache.size() + " protein elements into ProteinCache");
			}

			if (config.getString("nfile") != null) {
				myLogger.info("Parsing Nucleotide data");
				List<Nucleotide_Protein_Element> npList = npParser.parseFile(config.getString("nfile"));
				System.out.println("About to load " + npList.size() + " nucleotide elements into NucleiodCache");
				int loopCount = 0;
				nucleiodCache.startBatch();
				for (Nucleotide_Protein_Element x : npList) {
					nucleiodCache.put(x.getGenbankAccessionNumber(),x);
					loopCount++;
					if ((loopCount % 5000) == 0) {
						System.out.println("Added " + loopCount + " Nucleotide records");
						nucleiodCache.endBatch(true);
						nucleiodCache.startBatch();
					}
				}
				nucleiodCache.endBatch(true);
				System.out
						.println("Loaded " + nucleiodCache.size() + " nucleotide elements into NucleiodCache");
			}
			System.out.println("Parsing files....Done");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
