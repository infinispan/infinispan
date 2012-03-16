/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.ec2demo;

import com.martiansoftware.jsap.JSAPResult;
import org.infinispan.Cache;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.LegacyKeySupportSystemProperties;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author noconnor@redhat.com
 * 
 */
public class InfluenzaDataLoader {
	private CacheBuilder cbuilder;
	private Cache<String, Influenza_N_P_CR_Element> influenzaCache;
	private Cache<String, Nucleotide_Protein_Element> proteinCache;
	private Cache<String, Nucleotide_Protein_Element> nucleiodCache;
	private Nucleotide_Protein_Parser npParser;
	private Influenza_Parser iParser;

	private static final Log log = LogFactory.getLog(InfluenzaDataLoader.class);

	public void createCache(String configFile) throws IOException {
		String cfgFileName = LegacyKeySupportSystemProperties.getProperty("infinispan.configuration", "infinispan.demo.cfg");		
		if (cfgFileName == null)
			cfgFileName = configFile;
		
		cbuilder = new CacheBuilder(cfgFileName);
		influenzaCache = cbuilder.getCacheManager().getCache("InfluenzaCache");
		proteinCache = cbuilder.getCacheManager().getCache("ProteinCache");
		nucleiodCache = cbuilder.getCacheManager().getCache("NucleotideCache");
	}

	/**
	 * @param config
	 * @throws SAXException
	 */
	public void populateCache(JSAPResult config) throws SAXException {

		try {
			npParser = new Nucleotide_Protein_Parser();
			iParser = new Influenza_Parser();

			System.out.println("Caches created....Starting CacheManager");
			cbuilder.getCacheManager().start();

			int loadLimit = config.getInt("count");

			// Dump the cluster list
			List<Address> z = cbuilder.getCacheManager().getMembers();
			for (Address k : z)
				if (k != null)
					System.out.println("Cache Address=" + k.toString());

			System.out.println("Parsing files....");

			if (config.getString("ifile") != null) {
				log.info("Parsing Influenza data");
				List<Influenza_N_P_CR_Element> iList = iParser.parseFile(config.getString("ifile"));

				boolean rQuery = config.getBoolean("randomquery");
				int lSize = iList.size() - 1;

				if (rQuery) {
					System.out.println("Performing random queries");
					Random randomGenerator = new Random();
					while (true) {
						int currRec = randomGenerator.nextInt(lSize);
						Influenza_N_P_CR_Element curreElem = iList.get(currRec);

						this.searchCache(curreElem.getGanNucleoid());

						try {
							Thread.sleep(1000);
						} catch (InterruptedException ex) {
							// do nothing, yea I know its naughty...
						}
					}

				} else {
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
						if (loopCount == loadLimit) {
							System.out.println("Limited to " + loadLimit + " records");
							break;
						}
					}
					influenzaCache.endBatch(true);
					System.out.println("Loaded " + influenzaCache.size() + " influenza elements into influenzaCache");
				}
			}

			if (config.getString("pfile") != null) {
				log.info("Parsing Protein data");
				List<Nucleotide_Protein_Element> npList = npParser.parseFile(config.getString("pfile"));
				System.out.println("About to load " + npList.size() + " protein elements into ProteinCache");
				int loopCount = 0;
				proteinCache.startBatch();
				for (Nucleotide_Protein_Element x : npList) {
					proteinCache.put(x.getGenbankAccessionNumber(), x);
					loopCount++;
					if ((loopCount % 5000) == 0) {
						System.out.println("Added " + loopCount + " protein records");
						proteinCache.endBatch(true);
						proteinCache.startBatch();
					}
					if (loopCount == loadLimit) {
						System.out.println("Limited to " + loadLimit + " records");
						break;
					}
				}
				proteinCache.endBatch(true);
				System.out.println("Loaded " + proteinCache.size() + " protein elements into ProteinCache");
			}

			if (config.getString("nfile") != null) {
				log.info("Parsing Nucleotide data");
				List<Nucleotide_Protein_Element> npList = npParser.parseFile(config.getString("nfile"));
				System.out.println("About to load " + npList.size() + " nucleotide elements into NucleiodCache");
				int loopCount = 0;
				nucleiodCache.startBatch();
				for (Nucleotide_Protein_Element x : npList) {
					nucleiodCache.put(x.getGenbankAccessionNumber(), x);
					loopCount++;
					if ((loopCount % 5000) == 0) {
						System.out.println("Added " + loopCount + " Nucleotide records");
						nucleiodCache.endBatch(true);
						nucleiodCache.startBatch();
					}
					if (loopCount == loadLimit) {
						System.out.println("Limited to " + loadLimit + " records");
						break;
					}
				}
				nucleiodCache.endBatch(true);
				System.out.println("Loaded " + nucleiodCache.size() + " nucleotide elements into NucleiodCache");
			}
			System.out.println("Parsing files....Done");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void searchCache(String inGBAN) {
		log.trace("Searching influenzaCache for " + inGBAN);
		// Find the virus details
		Influenza_N_P_CR_Element myRec = influenzaCache.get(inGBAN);

		if (myRec != null) {
			System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			System.out.println("Virus Details->" + myRec);
			log.trace("Searching nucleiodCache for " + myRec.getGanNucleoid());
			Nucleotide_Protein_Element nucldet = nucleiodCache.get(myRec.getGanNucleoid());
			System.out.println("Nucleotide details->" + nucldet);

			// Display the protein details
			Map<String, String> myProt = myRec.getProtein_Data();
			for (String x : myProt.keySet()) {
				System.out.println("=========================================================================");
				log.trace("Searching proteinCache for " + x);
				Nucleotide_Protein_Element myProtdet = proteinCache.get(x);
				System.out.println("Protein->" + myProtdet);
				String protein_CR = myProt.get(x);
				System.out.println("Protein coding region->" + protein_CR);
			}
			System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		} else {
			log.trace("No virus data found for " + inGBAN);
			System.out.println("No virus data found for " + inGBAN);
		}
	}
	public String cacheSizes(){
      return "Protein/Influenza/Nucleotide Cache Size-->" + proteinCache.size() + "/"
      + influenzaCache.size() + "/" + nucleiodCache.size();
	}
}
