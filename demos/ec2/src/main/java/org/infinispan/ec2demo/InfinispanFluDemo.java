package org.infinispan.ec2demo;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import org.xml.sax.SAXException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * 
 */

/**
 * @author noconnor@redhat.com
 * 
 */
public class InfinispanFluDemo {

	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws JSAPException
	 * @throws SAXException
	 */
	public static void main(String[] args) throws JSAPException {
		SimpleJSAP jsap = new SimpleJSAP("InfinispanFluDemo", "Parse the Influenze data and store in cache ",
				new Parameter[] {
						new FlaggedOption("InfinispanCfg", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'c',
								JSAP.NO_LONGFLAG, "Location of Infinispan config file"),
						new FlaggedOption("ifile", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'i', JSAP.NO_LONGFLAG,
								"Location of influenza.dat"),
						new FlaggedOption("pfile", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'p', JSAP.NO_LONGFLAG,
								"location of influenza_aa.dat."),
						new Switch("query", 'q', "true", "Enable query cli"),
						new FlaggedOption("nfile", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'n', JSAP.NO_LONGFLAG,
								"Location of influenza_na.dat") });
		if (jsap.messagePrinted())
			System.exit(1);	
		
		
		JSAPResult config = jsap.parse(args);
		InfluenzaDataLoader fluDemo = new InfluenzaDataLoader();
		try {
			fluDemo.populateCache(config);
		} catch (SAXException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.exit(1);
		}

		while (true) {

			if (config.getBoolean("query")) {
				System.out.print("Enter Virus Genbank Accession Number: ");
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				String GBAN = null;
				try {
					GBAN = br.readLine();
				} catch (IOException ioe) {
					System.out.println("IO error trying to read Genbank Accession Number!");
					System.exit(1);
				}
				System.out.println("Searching cache...");

				// Find the virus details
				Influenza_N_P_CR_Element myRec = fluDemo.influenzaCache.get(GBAN);

				if (myRec != null) {
					System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
					System.out.println("Virus Details->" + myRec);
					Nucleotide_Protein_Element nucldet = fluDemo.nucleiodCache.get(myRec.getGanNucleoid());
					System.out.println("Nucleotide detils->" + nucldet);

					// Display the protein details
					Map<String, String> myProt = myRec.getProtein_Data();
					for (String x : myProt.keySet()) {
						Nucleotide_Protein_Element myProtdet = fluDemo.proteinCache.get(x);	
						System.out.println("Protein->" + myProtdet);
						String protein_CR = myProt.get(x);
						System.out.println("Protein coding region->" + protein_CR);
					}
					System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
				} else
					System.out.println("No virus found");
			} else {
				try {
					Thread.currentThread().sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			System.out.println("Protein Cache Size-->" + fluDemo.proteinCache.size());
			System.out.println("Influenza Cache Size-->" + fluDemo.influenzaCache.size());
			System.out.println("Nucleotide Cache Size-->" + fluDemo.nucleiodCache.size());
		}
	}
}
