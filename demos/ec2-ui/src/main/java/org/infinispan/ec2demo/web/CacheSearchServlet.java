/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.ec2demo.web;

import org.infinispan.Cache;
import org.infinispan.ec2demo.CacheBuilder;
import org.infinispan.ec2demo.Influenza_N_P_CR_Element;
import org.infinispan.ec2demo.Nucleotide_Protein_Element;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Servlet implementation class CacheSearchServlet
 */
public class CacheSearchServlet extends HttpServlet {
	private static final Log log = LogFactory.getLog(CacheSearchServlet.class);
	private static final long serialVersionUID = 1L;
	private Cache<String, Influenza_N_P_CR_Element> influenzaCache;
	private Cache<String, Nucleotide_Protein_Element> proteinCache;
	private Cache<String, Nucleotide_Protein_Element> nucleiodCache;

   @Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		CacheBuilder cacheBuilder = (CacheBuilder) getServletContext().getAttribute("cacheBuilder");
		influenzaCache = cacheBuilder.getCacheManager().getCache("InfluenzaCache");
		proteinCache = cacheBuilder.getCacheManager().getCache("ProteinCache");
		nucleiodCache = cacheBuilder.getCacheManager().getCache("NucleotideCache");
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
   @Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		RequestDispatcher dispatcher = getServletContext().getRequestDispatcher("/jsp/displayVirusDetails.jsp");
		dispatcher.forward(request, response);
	}

   @Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		CacheBuilder cacheBuilder = (CacheBuilder) getServletContext().getAttribute("cacheBuilder");
		String searchGBAN = request.getParameter("vGBAN");
		request.setAttribute("total", searchGBAN);
		
		//check the influenza cache first
		Influenza_N_P_CR_Element myRec = influenzaCache.get(searchGBAN);
		
		if (myRec != null) {
			log.trace("Searching nucleiodCache for " + myRec.getGanNucleoid());
			Nucleotide_Protein_Element nucldet = nucleiodCache.get(myRec.getGanNucleoid());			
			request.setAttribute("Nucleotide", nucldet);

			// Display the protein details
			Map<String, String> myProt = myRec.getProtein_Data();
			Map<String, String> myMap = new HashMap<String, String>();
			for (String x : myProt.keySet()) {
				log.trace("Searching proteinCache for " + x);
				Nucleotide_Protein_Element myProtdet = proteinCache.get(x);
				String protein_CR = myProt.get(x);		
				myMap.put(myProtdet.getGenbankAccessionNumber(), protein_CR);
			}
			request.setAttribute("PMap", myMap);
		} 



		//Retrieve the cache cluster memebers
		List<Address> myList = cacheBuilder.getCacheManager().getMembers();
		List<String> k = new ArrayList<String>();
		for (Address ad : myList)
			k.add(ad.toString());
		request.setAttribute("CMap", k);

		RequestDispatcher dispatcher = getServletContext().getRequestDispatcher("/jsp/VirusDetails.jsp");
		dispatcher.forward(request, response);
	}

}
