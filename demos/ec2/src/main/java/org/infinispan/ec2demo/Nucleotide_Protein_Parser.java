package org.infinispan.ec2demo;

import org.infinispan.commons.util.Util;
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
