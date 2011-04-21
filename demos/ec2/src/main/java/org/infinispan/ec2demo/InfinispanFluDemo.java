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

/**
 * @author noconnor@redhat.com
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
						new FlaggedOption("InfinispanCfg", JSAP.STRING_PARSER, null, JSAP.REQUIRED, 'c',
								JSAP.NO_LONGFLAG, "Location of Infinispan config file"),
						new FlaggedOption("ifile", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'i', JSAP.NO_LONGFLAG,
								"Location of influenza.dat"),
						new FlaggedOption("pfile", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'p', JSAP.NO_LONGFLAG,
								"location of influenza_aa.dat."),
						new FlaggedOption("count", JSAP.INTEGER_PARSER, "-1", JSAP.NOT_REQUIRED, 'l', JSAP.NO_LONGFLAG,
								"Number of records to load from file"),
						new Switch("query", 'q', "true", "Enable query cli"),
						new Switch("randomquery", 'r', "randomquery",
								"Randomly query the influenza to test that the cache is fully populated"),
						new FlaggedOption("nfile", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'n', JSAP.NO_LONGFLAG,
								"Location of influenza_na.dat") });
		if (jsap.messagePrinted())
			System.exit(1);

		JSAPResult config = jsap.parse(args);
		InfluenzaDataLoader fluDemo = new InfluenzaDataLoader();
		try {
			fluDemo.createCache(config.getString("InfinispanCfg"));
			fluDemo.populateCache(config);
		} catch (SAXException e1) {
			e1.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(2);
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

				fluDemo.searchCache(GBAN);

			} else {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			System.out.println(fluDemo.cacheSizes());
		}
	}

}
