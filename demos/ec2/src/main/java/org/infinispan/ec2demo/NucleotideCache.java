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
	
	public Cache getCache(){
		return myCache;
	}
}
