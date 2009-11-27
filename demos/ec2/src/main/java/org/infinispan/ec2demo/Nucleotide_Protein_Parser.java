package org.infinispan.ec2demo;

import org.milyn.Smooks;
import org.milyn.payload.JavaResult;
import org.xml.sax.SAXException;

import javax.xml.transform.stream.StreamSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

/**
 *
 */

/**
 * @author noconnor@redhat.com
 */
public class Nucleotide_Protein_Parser {

   public Nucleotide_Protein_Parser() {

   }

   @SuppressWarnings("unchecked")
   public List<Nucleotide_Protein_Element> parseFile(String fileName) throws IOException, SAXException {
      System.out.println("Parsing [" + fileName + "]");
      Smooks smooks = new Smooks("smooks-config.xml");

      try {
         JavaResult result = new JavaResult();
         smooks.filterSource(new StreamSource(new FileInputStream(fileName.trim())), result);
         return (List<Nucleotide_Protein_Element>) result.getBean("customerList");
      } finally {
         smooks.close();
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
