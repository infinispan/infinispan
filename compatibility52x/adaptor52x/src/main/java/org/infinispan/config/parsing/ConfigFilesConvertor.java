package org.infinispan.config.parsing;

import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.util.Util;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.sort;

/**
 * Class used for converting different configuration files to INFINISPAN format.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ConfigFilesConvertor {

   static final String JBOSS_CACHE3X = "JBossCache3x";
   static final String EHCACHE_CACHE1X = "Ehcache1x";
   static final String COHERENCE_35X = "Coherence35x";

   static final Map<String, String> TRANSFORMATIONS = new HashMap<String, String>(4);

   static {
      TRANSFORMATIONS.put(JBOSS_CACHE3X, "xslt/jbc3x2infinispan4.xslt");
      TRANSFORMATIONS.put(EHCACHE_CACHE1X, "xslt/ehcache1x2infinispan4x.xslt");
      TRANSFORMATIONS.put(COHERENCE_35X, "xslt/coherence35x2infinispan4x.xslt");
   }

   public void parse(InputStream is, OutputStream os, String xsltFile, ClassLoader cl) throws Exception {
      InputStream xsltInStream = FileLookupFactory.newInstance().lookupFile(xsltFile, cl);
      if (xsltInStream == null) {
         throw new IllegalStateException("Cold not find xslt file! : " + xsltFile);
      }

      try {
         Document document = getInputDocument(is);

         // Use a Transformer for output
         Transformer transformer = getTransformer(xsltInStream);

         DOMSource source = new DOMSource(document);
         ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         StreamResult result = new StreamResult(byteArrayOutputStream);
         transformer.transform(source, result);

         InputStream indentation = FileLookupFactory.newInstance().lookupFile("xslt/indent.xslt", cl);
         try {
            // Use a Transformer for output
            transformer = getTransformer(indentation);
            StreamResult finalResult = new StreamResult(os);
            StreamSource rawResult = new StreamSource(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
            transformer.transform(rawResult, finalResult);
         } finally {
            Util.close(indentation);
         }
      } finally {
         Util.close(xsltInStream);
      }
   }

   /**
    * Writes to the <b>os</b> the infinispan 4.x configuration file resulted by transforming configuration file passed
    * in as <b>inputFile</b>. Transformation is performed according to the <b>xsltFile</b>. Both <b>inputFile</b> and he
    * xslt file are looked up using a {@link org.infinispan.util.DefaultFileLookup}
    */
   public void parse(String inputFile, OutputStream os, String xsltFile, ClassLoader cl) throws Exception {
      InputStream stream = FileLookupFactory.newInstance().lookupFileStrict(inputFile, cl);
      try {
         parse(stream, os, xsltFile, cl);
      }
      finally {
         Util.close(stream);
      }
   }

   private static void help() {
      System.out.println("Usage:");
      System.out.println("importConfig [-source <the file to be transformed>] [-destination <where to store resulting XML>] [-type <the type of the source, possible values being: " + getSupportedFormats() + " >]");
   }


   /**
    * usage : java org.jboss.cache.config.parsing.ConfigFilesConvertor -Dsource=config-2.x.xml
    * -Ddestination=config-3.x.xnl
    */
   public static void main(String[] args) throws Exception {
      String sourceName = null, destinationName = null, type = null;
      for (int i = 0; i < args.length; i++) {
         if (args[i].equals("-source")) {
            sourceName = args[++i];
            continue;
         }
         if (args[i].equals("-destination")) {
            destinationName = args[++i];
            continue;
         }
         if (args[i].equals("-type")) {
            type = args[++i];
            continue;
         }
         help();
      }

      mustExist(sourceName, "source");
      mustExist(destinationName, "destination");
      mustExist(type, "type");

      if (!TRANSFORMATIONS.containsKey(type)) {
         System.err.println("Unsupported transformation type: " + type + ". Supported formats are: " + getSupportedFormats());
      }

      if (type.equals(JBOSS_CACHE3X)) {
         transformFromJbossCache3x(sourceName, destinationName, ConfigFilesConvertor.class.getClassLoader());
      } else {
         transformFromNonJBoss(sourceName, destinationName, TRANSFORMATIONS.get(type), ConfigFilesConvertor.class.getClassLoader());
      }

      System.out.println("---");
      System.out.println("New configuration file [" + destinationName + "] successfully created.");
      System.out.println("---");
   }

   private static String getSupportedFormats() {
      List<String> supported = new LinkedList<String>(TRANSFORMATIONS.keySet());
      sort(supported);
      return supported.toString();
   }

   private static void transformFromNonJBoss(String sourceName, String destinationName, String xslt, ClassLoader cl) throws Exception {
      File oldConfig = new File(sourceName);
      if (!oldConfig.exists()) {
         System.err.println("File specified as input ('" + sourceName + ") does not exist.");
         System.exit(1);
      }
      ConfigFilesConvertor convertor = new ConfigFilesConvertor();
      FileInputStream is = null;
      FileOutputStream fos = null;

      try {

         is = new FileInputStream(oldConfig);
         File destination = new File(destinationName);
         if (!destination.exists()) {
            if (!destination.createNewFile()) {
               System.err.println("Warn! Could not create file " + destination);
            }
         }

         fos = new FileOutputStream(destinationName);
         convertor.parse(is, fos, xslt, cl);
      } finally {
         Util.flushAndCloseStream(fos);
         Util.close(is);
      }
   }

   private static void mustExist(String sourceName, String what) {
      if (sourceName == null) {
         System.err.println("Missing '" + what + "', cannot proceed");
         help();
         System.exit(1);
      }
   }

   private static void transformFromJbossCache3x(String sourceName, String destinationName, ClassLoader cl) throws Exception {
      File oldConfig = new File(sourceName);
      if (!oldConfig.exists()) {
         System.err.println("File specified as input ('" + sourceName + ") does not exist.");
         System.exit(1);
      }
      ConfigFilesConvertor convertor = new ConfigFilesConvertor();
      FileInputStream is = null;
      FileOutputStream fos = null;
      File jgroupsConfigFile = new File("jgroupsConfig.xml");
      try {
         is = new FileInputStream(oldConfig);
         File destination = new File(destinationName);
         if (!destination.exists()) {
            if (!destination.createNewFile()) {
               System.err.println("Problems creating destination file: " + destination);
            }
         }
         fos = new FileOutputStream(destinationName);
         convertor.parse(is, fos, "xslt/jbc3x2infinispan4x.xslt", cl);
         fos.close();
         is.close();


         if (jgroupsConfigFile.exists())
            if (!jgroupsConfigFile.delete()) {
               System.err.println("Problems deleting existing jgroups file: " + jgroupsConfigFile);
            }
         if (!jgroupsConfigFile.createNewFile()) {
            System.err.println("Could not create jgroupsConfigFile: " + jgroupsConfigFile);
         }
         is = new FileInputStream(oldConfig);
         fos = new FileOutputStream(jgroupsConfigFile);
         convertor = new ConfigFilesConvertor();
         convertor.parse(is, fos, "xslt/jgroupsFileGen.xslt", cl);
      } finally {
         Util.close(is);
         Util.flushAndCloseStream(fos);
      }

      //now this means that the generated file is basically empty, so delete ie
      if (jgroupsConfigFile.length() < 5) {
         jgroupsConfigFile.delete();
      }
   }

   private Transformer getTransformer(InputStream xsltInStream) throws TransformerConfigurationException {
      TransformerFactory tFactory = TransformerFactory.newInstance();
      StreamSource stylesource = new StreamSource(xsltInStream);
      return tFactory.newTransformer(stylesource);
   }

   private Document getInputDocument(InputStream is) throws ParserConfigurationException, SAXException, IOException {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      return builder.parse(is);
   }
}