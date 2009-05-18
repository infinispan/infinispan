/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.config;

import org.infinispan.util.FileLookup;
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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayInputStream;

/**
 * Class used for converting different configuration files to INFINISPAN format.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ConfigFilesConvertor {

   public void parse(InputStream is, OutputStream os, String xsltFile) throws Exception {
      InputStream xsltInStream = new FileLookup().lookupFile(xsltFile);
      if (xsltInStream == null) {
         throw new IllegalStateException("Cold not find xslt file! : " + xsltFile);
      }

      Document document = getInputDocument(is);

      // Use a Transformer for output
      Transformer transformer = getTransformer(xsltInStream);

      DOMSource source = new DOMSource(document);
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      StreamResult result = new StreamResult(byteArrayOutputStream);
      transformer.transform(source, result);

      InputStream indentation = new FileLookup().lookupFile("C:\\jboss\\ifspn\\zzzz_trunk_pristine\\src\\main\\resources\\xslt\\indent.xslt");
      // Use a Transformer for output
      transformer = getTransformer(indentation);
      StreamResult finalResult = new StreamResult(os);
      StreamSource rawResult = new StreamSource(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
      transformer.transform(rawResult, finalResult);
      xsltInStream.close();
   }

   /**
    * Writes to the <b>os</b> the 3.x configuration file resulted by transforming the 2.x configuration file passed in
    * as <b>inputFile</b>. Transformation is performed according to the <b>xsltFile</b>. Both <b>inputFile</b> and he
    * xslt file are looked up using a {@link org.jboss.cache.util.FileLookup}
    */
   public void parse(String inputFile, OutputStream os, String xsltFile) throws Exception {
      InputStream stream = new FileLookup().lookupFile(inputFile);
      try {
         parse(stream, os, xsltFile);
      }
      finally {
         stream.close();
      }
   }

   /**
    * usage : java org.jboss.cache.config.parsing.ConfigFilesConvertor -Dsource=config-2.x.xml
    * -Ddestination=config-3.x.xnl
    */
   public static void main(String[] argv) throws Exception {
      String sourceName = System.getProperty("source");
      if (sourceName == null) {
         System.err.println("Missing property 'source'.");
         System.exit(1);
      }
      String destinationName = System.getProperty("destination");
      if (destinationName == null) {
         System.err.println("Missing property 'destination'.");
         System.exit(1);
      }
      File oldConfig = new File(sourceName);
      if (!oldConfig.exists()) {
         System.err.println("File specified as input ('" + sourceName + ") does not exist.");
         System.exit(1);
      }
      ConfigFilesConvertor convertor = new ConfigFilesConvertor();
      FileInputStream is = new FileInputStream(oldConfig);
      File destination = new File(destinationName);
      if (!destination.exists()) destination.createNewFile();
      FileOutputStream fos = new FileOutputStream(destinationName);
      convertor.parse(is, fos, "C:\\jboss\\ifspn\\zzzz_trunk_pristine\\src\\main\\resources\\xslt\\jbc3x2infinispan4x.xslt");
      fos.close();
      is.close();

      String jgroupsConfig = destination.getParent() + File.separator + "jgroupsConfig.xml";
      File jgroupsConfigFile = new File(jgroupsConfig);
      if (jgroupsConfigFile.exists()) jgroupsConfigFile.delete();
      jgroupsConfigFile.createNewFile();
      is = new FileInputStream(oldConfig);
      fos = new FileOutputStream(jgroupsConfigFile);
      convertor = new ConfigFilesConvertor();
      convertor.parse(is, fos, "C:\\jboss\\ifspn\\zzzz_trunk_pristine\\src\\main\\resources\\xslt\\jgroupsFileGen.xslt");
      is.close();
      fos.close();

      System.out.println("jgroupsConfigFile.length() = " + jgroupsConfigFile.length());
      //now this means that the generated file is basically empty, so delete ie
      if (jgroupsConfigFile.length() < 5) {
         jgroupsConfigFile.delete();
      }
      System.out.println("---");
      System.out.println("New configuration file [" + destinationName + "] successfully created.");
      System.out.println("---");
   }

   private Transformer getTransformer(InputStream xsltInStream) throws TransformerConfigurationException {
      TransformerFactory tFactory = TransformerFactory.newInstance();
      StreamSource stylesource = new StreamSource(xsltInStream);
      return tFactory.newTransformer(stylesource);
   }

   private Document getInputDocument(InputStream is)  throws ParserConfigurationException, SAXException, IOException {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      return builder.parse(is);
   }
}