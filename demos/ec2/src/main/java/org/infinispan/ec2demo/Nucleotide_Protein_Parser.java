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

import org.infinispan.util.Util;
import org.milyn.Smooks;
import org.milyn.payload.JavaResult;
import org.xml.sax.SAXException;

import javax.xml.transform.stream.StreamSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

/**
 * @author noconnor@redhat.com
 */
public class Nucleotide_Protein_Parser {

   @SuppressWarnings("unchecked")
   public List<Nucleotide_Protein_Element> parseFile(String fileName) throws IOException, SAXException {
      System.out.println("Parsing [" + fileName + "]");
      Smooks smooks = new Smooks("config-samples/ec2-demo/smooks-config.xml");

      FileInputStream inputStream = null;
      try {
         JavaResult result = new JavaResult();
         inputStream = new FileInputStream(fileName.trim());
         smooks.filterSource(new StreamSource(inputStream), result);
         return (List<Nucleotide_Protein_Element>) result.getBean("customerList");
      } finally {
         smooks.close();
         Util.close(inputStream);
      }
   }

   public void processFile(String fileName, ProteinCache cacheImpl) {
      if (fileName == null) {
         System.out.println("No file to process...");
         return;
      }
      System.out.println("Processing file " + fileName);

      try {
         List<Nucleotide_Protein_Element> myData = parseFile(fileName);
         for (Nucleotide_Protein_Element x : myData) {
            cacheImpl.addToCache(x);
         }
         System.out.println("Processed " + myData.size() + " records from file...");
         System.out.println("Number stored in cache=" + cacheImpl.getCacheSize());
      } catch (IOException e) {
         e.printStackTrace();
      } catch (SAXException e) {
         e.printStackTrace();
      }
   }
}
