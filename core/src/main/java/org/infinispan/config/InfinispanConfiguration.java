/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
 
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;

import org.infinispan.config.parsing.XmlConfigurationParser;
import org.infinispan.util.FileLookup;
/**
 * InfinispanConfiguration encapsulates root component of Infinispan XML configuration
 * 
 * <p>
 * Note that class InfinispanConfiguration contains JAXB annotations. These annotations determine how XML
 * configuration files are read into instances of configuration class hierarchy as well as they
 * provide meta data for configuration file XML schema generation. Please modify these annotations
 * and Java element types they annotate with utmost understanding and care.
 *
 * @author Vladimir Blagojevic
 * @since 4.0
 */

@XmlRootElement(name = "infinispan")
@XmlAccessorType(XmlAccessType.FIELD)
public class InfinispanConfiguration implements XmlConfigurationParser {
   
   @XmlElement
   private GlobalConfiguration global;
   
   @XmlElement(name="default")
   private Configuration defaultConfiguration;
   
   @XmlElement(name="namedCache")
   private List<Configuration> namedCaches;
   
   /**
    * Factory method to create an instance of Infinispan configuration. If users want to verify
    * configuration file correctness against Infinispan schema then appropriate schema file name
    * should be provided as well.
    * <p>
    * Both configuration file and schema file are looked up in following order:
    * <p>
    *  <ol> 
    *  <li> using current thread's context ClassLoader</li> 
    *  <li> if fails, the system ClassLoader</li> 
    *  <li> if fails, attempt is made to load it as a file from the disk </li> 
    *  </ol> 
    * 
    * @param configFileName configuration file name
    * @param schemaFileName schema file name
    * @return infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object 
    */
   public static InfinispanConfiguration newInfinispanConfiguration(String configFileName,
            String schemaFileName) throws IOException {

      InputStream inputStream = configFileName != null ? findInputStream(configFileName) : null;
      InputStream schemaIS = schemaFileName != null ? findInputStream(schemaFileName) : null;
      return newInfinispanConfiguration(inputStream, schemaIS);
   }
   
   /**
    * Factory method to create an instance of Infinispan configuration. 
    * <p>
    * Configuration file is looked up in following order:
    * <p>
    *  <ol> 
    *  <li> using current thread's context ClassLoader</li> 
    *  <li> if fails, the system ClassLoader</li> 
    *  <li> if fails, attempt is made to load it as a file from the disk </li> 
    *  </ol> 
    * 
    * @param configFileName configuration file name
    * @return returns infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object 
    */
   public static InfinispanConfiguration newInfinispanConfiguration(String configFileName)
            throws IOException {
      return newInfinispanConfiguration(configFileName,null);
     
   }
   
   /**
    * Factory method to create an instance of Infinispan configuration. 
    * 
    * @param config configuration input stream
    * @return returns infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object 
    */
   public static InfinispanConfiguration newInfinispanConfiguration(InputStream config)
            throws IOException {
      return newInfinispanConfiguration(config, null); 
   }
   
   /**
    * Factory method to create an instance of Infinispan configuration. If users want to verify
    * configuration file correctness against Infinispan schema then appropriate schema input stream
    * should be provided as well.
    *
    * @param config configuration input stream
    * @param schema schema inputstream
    * @return infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object 
    */
   public static InfinispanConfiguration newInfinispanConfiguration(InputStream config,
            InputStream schema) throws IOException {
      try {
         JAXBContext jc = JAXBContext.newInstance(InfinispanConfiguration.class);
         Unmarshaller u = jc.createUnmarshaller();
         if (schema != null) {
            SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
            u.setSchema(factory.newSchema(new StreamSource(schema)));
         }
         InfinispanConfiguration doc = (InfinispanConfiguration) u.unmarshal(config);
         //legacy, don't ask
         doc.parseGlobalConfiguration().setDefaultConfiguration(doc.parseDefaultConfiguration());
         return doc;
      } catch (Exception e) {
         IOException ioe = new IOException(e.getLocalizedMessage());
         ioe.initCause(e);
         throw ioe;
      }
   }

   /**
    * Should never called. Construct InfinispanConfiguration with constructor other than no-arg constructor
    * <p>
    * Needed for reflection
    * 
    */
   public InfinispanConfiguration() {
      super();
   }
  
   private static InputStream findInputStream(String fileName) throws FileNotFoundException {
      if (fileName == null)
         throw new NullPointerException("File name cannot be null!");
      FileLookup fileLookup = new FileLookup();
      InputStream is = fileLookup.lookupFile(fileName);
      if (is == null)
         throw new FileNotFoundException("File " + fileName
                  + " could not be found, either on the classpath or on the file system!");
      return is;
   } 

   public Configuration parseDefaultConfiguration() throws ConfigurationException {
      return defaultConfiguration;
   }

   public GlobalConfiguration parseGlobalConfiguration() {
      return  global;
   }

   public Map<String, Configuration> parseNamedConfigurations() throws ConfigurationException {
      Map<String,Configuration> map = new HashMap<String, Configuration>(7);
      for (Configuration conf : namedCaches) {
         map.put(conf.getName(), conf);
      }
      return map;
   }
}
