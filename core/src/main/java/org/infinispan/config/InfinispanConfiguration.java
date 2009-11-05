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

import org.infinispan.Version;
import org.infinispan.config.parsing.XmlConfigurationParser;
import org.infinispan.util.FileLookup;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.util.StringPropertyReplacer;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.ValidationEvent;
import javax.xml.bind.ValidationEventHandler;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * InfinispanConfiguration encapsulates root component of Infinispan XML configuration
 * <p/>
 * <p/>
 * Note that class InfinispanConfiguration contains JAXB annotations. These annotations determine how XML configuration
 * files are read into instances of configuration class hierarchy as well as they provide meta data for configuration
 * file XML schema generation. Please modify these annotations and Java element types they annotate with utmost
 * understanding and care.
 * 
 * @configRef name="infinispan",desc="Root of Infinispan configuration."
 *
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@XmlRootElement(name = "infinispan")
@XmlAccessorType(XmlAccessType.FIELD)
public class InfinispanConfiguration implements XmlConfigurationParser {

   private static final Log log = LogFactory.getLog(InfinispanConfiguration.class);

   public static final String VALIDATING_SYSTEM_PROPERTY = "infinispan.config.validate";

   public static final String SKIP_TOKEN_REPLACEMENT = "infinispan.config.skipTokenReplacement";

   public static final String SCHEMA_SYSTEM_PROPERTY = "infinispan.config.schema";

   private static final String DEFAULT_SCHEMA_LOCATION = "schema/infinispan-config-" + Version.getMajorVersion() + ".xsd";

   public static final String SCHEMA_URL_SYSTEM_PROPERTY = "infinispan.config.schema.url";

   private static final String DEFAULT_SCHEMA_URL = "http://www.jboss.org/infinispan/infinispan-config-" + Version.getMajorVersion() + ".xsd";

   @XmlElement
   private final GlobalConfiguration global = new GlobalConfiguration();

   @XmlElement(name = "default")
   private final Configuration defaultConfiguration = new Configuration();

   @XmlElement(name = "namedCache")
   private final List<Configuration> namedCaches = new ArrayList<Configuration>();

   /**
    * Factory method to create an instance of Infinispan configuration. If users want to verify configuration file
    * correctness against Infinispan schema then appropriate schema file name should be provided as well.
    * <p/>
    * Both configuration file and schema file are looked up in following order:
    * <p/>
    * <ol> <li> using current thread's context ClassLoader</li> <li> if fails, the system ClassLoader</li> <li> if
    * fails, attempt is made to load it as a file from the disk </li> </ol>
    *
    * @param configFileName configuration file name
    * @param schemaFileName schema file name
    * @return infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object
    */
   public static InfinispanConfiguration newInfinispanConfiguration(String configFileName,
            String schemaFileName) throws IOException {
      return newInfinispanConfiguration(configFileName, schemaFileName, null);
   }
   
   /**
    * Factory method to create an instance of Infinispan configuration. If users want to verify configuration file
    * correctness against Infinispan schema then appropriate schema file name should be provided as well.
    * <p/>
    * Both configuration file and schema file are looked up in following order:
    * <p/>
    * <ol> <li> using current thread's context ClassLoader</li> <li> if fails, the system ClassLoader</li> <li> if
    * fails, attempt is made to load it as a file from the disk </li> </ol>
    *
    * @param configFileName configuration file name
    * @param schemaFileName schema file name
    * @param cbv configuration bean visitor passed to constructed InfinispanConfiguration
    * 
    * @return infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object
    */
   public static InfinispanConfiguration newInfinispanConfiguration(String configFileName,
            String schemaFileName, ConfigurationBeanVisitor cbv) throws IOException {

      InputStream inputStream = configFileName != null ? findInputStream(configFileName) : null;
      InputStream schemaIS = schemaFileName != null ? findInputStream(schemaFileName) : null;
      return newInfinispanConfiguration(inputStream, schemaIS, cbv);
   }

   /**
    * Factory method to create an instance of Infinispan configuration.
    * <p/>
    * Configuration file is looked up in following order:
    * <p/>
    * <ol> <li> using current thread's context ClassLoader</li> <li> if fails, the system ClassLoader</li> <li> if
    * fails, attempt is made to load it as a file from the disk </li> </ol>
    *
    * @param configFileName configuration file name
    * @return returns infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object
    */
   public static InfinispanConfiguration newInfinispanConfiguration(String configFileName)
         throws IOException {
      return newInfinispanConfiguration(configFileName, null);
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
    * Factory method to create an instance of Infinispan configuration. If users want to verify configuration file
    * correctness against Infinispan schema then appropriate schema input stream should be provided as well.
    *
    * @param config configuration input stream
    * @param schema schema inputstream
    * 
    * @return infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object
    */
   public static InfinispanConfiguration newInfinispanConfiguration(InputStream config,
                                                                    InputStream schema) throws IOException {
      return newInfinispanConfiguration(config,schema,null);
   }
   
   /**
    * Factory method to create an instance of Infinispan configuration. If users want to verify configuration file
    * correctness against Infinispan schema then appropriate schema input stream should be provided as well.
    *
    * @param config configuration input stream
    * @param schema schema inputstream
    * @param cbv configuration bean visitor passed to constructed InfinispanConfiguration
    * 
    * @return infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object
    */
   public static InfinispanConfiguration newInfinispanConfiguration(InputStream config,
            InputStream schema, ConfigurationBeanVisitor cbv) throws IOException {
      try {
         JAXBContext jc = JAXBContext.newInstance(InfinispanConfiguration.class);
         Unmarshaller u = jc.createUnmarshaller();         

         if (schema != null) {
            SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
            u.setSchema(factory.newSchema(new StreamSource(schema)));
            u.setEventHandler(new ValidationEventHandler() {
               @Override
               public boolean handleEvent(ValidationEvent event) {               
                  int severity = event.getSeverity();
                  return (severity != ValidationEvent.FATAL_ERROR && severity != ValidationEvent.ERROR);
               }
            });
         }
         
         InputSource source = null;
         if (skipTokenReplacement()) {
            source = new InputSource(config);
         } else {
            source = replaceProperties(config);
         }
         DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
         dbf.setNamespaceAware(true);
         DocumentBuilder db = dbf.newDocumentBuilder();
         Document document = db.parse(source);
         InfinispanConfiguration ic = (InfinispanConfiguration) u.unmarshal(document);     
         
         // legacy, don't ask
         GlobalConfiguration gconf = ic.parseGlobalConfiguration();
         gconf.setDefaultConfiguration(ic.parseDefaultConfiguration());
         if (cbv != null) {
            ic.accept(cbv);
         }
         ic.accept(new ModuleConfigurationResolverVisitor(document));
         return ic;
      } catch (ConfigurationException cex) {
         throw cex;
      } catch (NullPointerException npe) {
         throw npe;
      } catch (Exception e) {
         IOException ioe = new IOException(e.getLocalizedMessage());
         ioe.initCause(e);
         throw ioe;
      }
   }

   private static InputSource replaceProperties(InputStream config) throws Exception{
      BufferedReader br = new BufferedReader ( new InputStreamReader(config));
      StringBuilder w = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
         int dollar = line.indexOf('$');
         if(dollar >0 && line.indexOf('{',dollar) > 0 && line.indexOf('}',dollar)>0) {
            String replacedLine = StringPropertyReplacer.replaceProperties(line);
            if (line.equals(replacedLine)) {
               log.warn("Property " +line.substring(line.indexOf('{')+1,line.indexOf('}')) + " could not be replaced as intended!");
            }
            w.append(replacedLine);
         } else {
            w.append(line);
         }         
      }
      return new InputSource(new StringReader(w.toString()));
   }

   private static boolean skipSchemaValidation() {
      String s = System.getProperty(VALIDATING_SYSTEM_PROPERTY);
      return s != null && !Boolean.parseBoolean(s);
   }
   
   private static boolean skipTokenReplacement() {
      String s = System.getProperty(SKIP_TOKEN_REPLACEMENT, "false");
      return s != null && Boolean.parseBoolean(s);
   }

   public static InputStream findSchemaInputStream() {
      boolean validating = !skipSchemaValidation();
      if (!validating)
         return null;

      FileLookup fileLookup = new FileLookup();
      InputStream is = fileLookup.lookupFile(schemaPath());
      if (is != null)
         return is;
      try {
         is = new URL(schemaURL()).openStream();
         return is;
      } catch (Exception e) {
      }
      return null;
   }

   public static String resolveSchemaPath() {
      boolean validating = !skipSchemaValidation();
      if (!validating)
         return null;
      return schemaPath();
   }

   private static String schemaPath() {
      return System.getProperty(SCHEMA_SYSTEM_PROPERTY, DEFAULT_SCHEMA_LOCATION);
   }

   private static String schemaURL() {
      return System.getProperty(SCHEMA_URL_SYSTEM_PROPERTY, DEFAULT_SCHEMA_URL);
   }

   /**
    * Should never called. Construct InfinispanConfiguration with constructor other than no-arg constructor
    * <p/>
    * Needed for reflection
    */
   public InfinispanConfiguration() {
      super();
   }
   
   public void accept(ConfigurationBeanVisitor v) {
      global.accept(v);
      defaultConfiguration.accept(v);      
      for (Configuration c : namedCaches) {
         c.accept(v);
      }            
      v.visitInfinispanConfiguration(this);
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
      return global;
   }

   public Map<String, Configuration> parseNamedConfigurations() throws ConfigurationException {      
      Map<String, Configuration> map = new HashMap<String, Configuration>(7);
      for (Configuration conf : namedCaches) {
         map.put(conf.getName(), conf);
      }
      return map;
   }
}
