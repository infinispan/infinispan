package org.infinispan.config;

import org.infinispan.Version;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.StringPropertyReplacer;
import org.infinispan.commons.util.SysPropertyActions;
import org.infinispan.config.parsing.NamespaceFilter;
import org.infinispan.config.parsing.XmlConfigurationParser;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import javax.xml.bind.*;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;
import java.io.*;
import java.lang.ref.SoftReference;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * InfinispanConfiguration encapsulates root component of Infinispan XML configuration. Can be empty
 * for sensible defaults throughout, however that would only give you the most basic of local,
 * non-clustered caches.
 *
 * @author Vladimir Blagojevic
 * @since 4.0
 * @deprecated Use {@link ParserRegistry} instead
 */
@XmlRootElement(name = "infinispan")
@XmlAccessorType(XmlAccessType.FIELD)
@ConfigurationDoc(name = "infinispan")
@Deprecated
public class InfinispanConfiguration implements XmlConfigurationParser, JAXBUnmarshallable {

   private static final Log log = LogFactory.getLog(InfinispanConfiguration.class);

   public static final String VALIDATING_SYSTEM_PROPERTY = "infinispan.config.validate";

   public static final String SKIP_TOKEN_REPLACEMENT = "infinispan.config.skipTokenReplacement";

   public static final String SCHEMA_SYSTEM_PROPERTY = "infinispan.config.schema";

   private static final String DEFAULT_SCHEMA_LOCATION = String.format("schema/infinispan-config-%s.xsd", Version.MAJOR_MINOR);

   public static final String SCHEMA_URL_SYSTEM_PROPERTY = "infinispan.config.schema.url";

   private static final String DEFAULT_SCHEMA_URL = String.format("http://www.infinispan.org/schemas/infinispan-config-%s.xsd", Version.MAJOR_MINOR);

   private static SoftReference<JAXBContext> jaxbContextReference;

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
    * <ol> <li> using the specified ClassLoader</li> <li> if fails, the system ClassLoader</li> <li> if
    * fails, attempt is made to load it as a file from the disk </li> </ol>
    *
    * @param configFileName configuration file name
    * @param schemaFileName schema file name
    * @return infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object
    */
   public static InfinispanConfiguration newInfinispanConfiguration(String configFileName,
                                                                    String schemaFileName, ClassLoader cl) throws IOException {
      return newInfinispanConfiguration(configFileName, schemaFileName, null, cl);
   }

   /**
    * Factory method to create an instance of Infinispan configuration. If users want to verify configuration file
    * correctness against Infinispan schema then appropriate schema file name should be provided as well.
    * <p/>
    * Both configuration file and schema file are looked up in following order:
    * <p/>
    * <ol> <li> using specified ClassLoader</li> <li> if fails, the system ClassLoader</li> <li> if
    * fails, attempt is made to load it as a file from the disk </li> </ol>
    *
    * @param configFileName configuration file name
    * @param schemaFileName schema file name
    * @param cbv            configuration bean visitor passed to constructed InfinispanConfiguration
    * @return infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object
    */
   public static InfinispanConfiguration newInfinispanConfiguration(String configFileName,
                                                                    String schemaFileName, ConfigurationBeanVisitor cbv, ClassLoader cl) throws IOException {

      InputStream inputStream = configFileName != null ? findInputStream(configFileName, cl) : null;
      InputStream schemaIS = schemaFileName != null ? findSchemaInputStream(schemaFileName) : null;
      try {
         return newInfinispanConfiguration(inputStream, schemaIS, cbv);
      } finally {
         Util.close(inputStream, schemaIS);
      }
   }

   /**
    * Factory method to create an instance of Infinispan configuration.
    * <p/>
    * Configuration file is looked up in following order:
    * <p/>
    * <ol> <li> using the specified ClassLoader</li> <li> if fails, the system ClassLoader</li> <li> if
    * fails, attempt is made to load it as a file from the disk </li> </ol>
    *
    * @param configFileName configuration file name
    * @return returns infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object
    */
   public static InfinispanConfiguration newInfinispanConfiguration(String configFileName, ClassLoader cl)
           throws IOException {
      return newInfinispanConfiguration(configFileName, null, cl);
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
    * Factory method to create an instance of Infinispan configuration.
    *
    * @param config configuration reader
    * @return returns infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object
    */
   public static InfinispanConfiguration newInfinispanConfiguration(Reader config)
           throws IOException {
      return newInfinispanConfiguration(config, null);
   }

   /**
    * Factory method to create an instance of Infinispan configuration. If users want to verify configuration file
    * correctness against Infinispan schema then appropriate schema input stream should be provided as well.
    *
    * @param config configuration input stream
    * @param schema schema inputstream
    * @return infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object
    */
   public static InfinispanConfiguration newInfinispanConfiguration(InputStream config,
                                                                    InputStream schema) throws IOException {
      return newInfinispanConfiguration(config, schema, null);
   }

   /**
    * Factory method to create an instance of Infinispan configuration. If users want to verify configuration file
    * correctness against Infinispan schema then appropriate schema input stream should be provided as well.
    *
    * @param config configuration reader
    * @param schema schema inputstream
    * @return infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object
    */
   public static InfinispanConfiguration newInfinispanConfiguration(Reader config,
                                                                    InputStream schema) throws IOException {
      return newInfinispanConfiguration(config, schema, null);
   }

   /**
    * Factory method to create an instance of Infinispan configuration. If users want to verify configuration file
    * correctness against Infinispan schema then appropriate schema input stream should be provided as well.
    *
    * @param config configuration reader
    * @param schema schema inputstream
    * @param cbv    configuration bean visitor passed to constructed InfinispanConfiguration
    * @return infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object
    */
   public static InfinispanConfiguration newInfinispanConfiguration(Reader config,
                                                                    InputStream schema, ConfigurationBeanVisitor cbv) throws IOException {
      try {
         Unmarshaller u = getJAXBContext().createUnmarshaller();
         NamespaceFilter nf = new NamespaceFilter();
         XMLReader reader = XMLReaderFactory.createXMLReader();
         nf.setParent(reader);

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

         SAXSource source;
         if (skipTokenReplacement()) {
            source = new SAXSource(nf, new InputSource(config));
         } else {
            source = replaceProperties(config, nf);
         }

         u.setListener(new Unmarshaller.Listener() {
            @Override
            public void beforeUnmarshal(Object target, Object parent) {
               if (target instanceof JAXBUnmarshallable) {
                  // notify the bean that it is about to be unmarshalled
                  ((JAXBUnmarshallable) target).willUnmarshall(parent);
               }
            }
         });

         InfinispanConfiguration ic = (InfinispanConfiguration) u.unmarshal(source);
         ic.accept(cbv);
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

   /**
    * Factory method to create an instance of Infinispan configuration. If users want to verify configuration file
    * correctness against Infinispan schema then appropriate schema input stream should be provided as well.
    *
    * @param config configuration input stream
    * @param schema schema inputstream
    * @param cbv    configuration bean visitor passed to constructed InfinispanConfiguration
    * @return infinispan configuration
    * @throws IOException if there are any issues creating InfinispanConfiguration object
    */
   public static InfinispanConfiguration newInfinispanConfiguration(InputStream config,
                                                                    InputStream schema, ConfigurationBeanVisitor cbv) throws IOException {
      return newInfinispanConfiguration(new InputStreamReader(config), schema, cbv);
   }

   /**
    * Returns the JAXB context that may be used to read and write Infinispan configurations.
    *
    * @return the JAXB context that may be used to read and write Infinispan configurations.
    * @throws JAXBException In case of the creation of the context failed.
    */
   protected static synchronized JAXBContext getJAXBContext() throws JAXBException {
      JAXBContext context = jaxbContextReference != null ? jaxbContextReference.get() : null;
      if (context == null) {
         context = JAXBContext.newInstance(InfinispanConfiguration.class);
         jaxbContextReference = new SoftReference<JAXBContext>(context);
      }

      return context;
   }

   /**
    * Converts an instance of GlobalConfiguration, Configuration or InfinispanConfiguration to
    * its XML representation.
    *
    * @param compatibleConfigurationInstance
    *         a configuration instance that support XML marshaling.
    * @return a string containing the formatted XML representation of the given instance.
    */
   protected static String toXmlString(Object compatibleConfigurationInstance) {
      try {
         StringWriter writer = new StringWriter(1024);
         try {
            Marshaller m = getJAXBContext().createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            m.marshal(compatibleConfigurationInstance, writer);
            return writer.toString();
         } finally {
            writer.close();
         }
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private static SAXSource replaceProperties(Reader config, XMLFilter filter) throws Exception {
      BufferedReader br = new BufferedReader(config);
      StringBuilder w = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
         int dollar = line.indexOf('$');
         if (dollar > 0 && line.indexOf('{', dollar) > 0 && line.indexOf('}', dollar) > 0) {
            String replacedLine = StringPropertyReplacer.replaceProperties(line);
            if (line.equals(replacedLine)) {
               log.propertyCouldNotBeReplaced(line.substring(line.indexOf('{') + 1, line.indexOf('}')));
            }
            w.append(replacedLine);
         } else {
            w.append(line);
         }
      }

      return new SAXSource(filter, new InputSource(new StringReader(w.toString())));
   }

   private static boolean skipSchemaValidation() {
      String s = SysPropertyActions.getProperty(VALIDATING_SYSTEM_PROPERTY);
      return s != null && !Boolean.parseBoolean(s);
   }

   private static boolean skipTokenReplacement() {
      String s = SysPropertyActions.getProperty(SKIP_TOKEN_REPLACEMENT, "false");
      return s != null && Boolean.parseBoolean(s);
   }

   public static InputStream findSchemaInputStream() {
      return findSchemaInputStream(null);
   }

   public static InputStream findSchemaInputStream(String localPathToSchema) {
      boolean validating = !skipSchemaValidation();
      if (!validating)
         return null;

      //1. resolve given path
      FileLookup fileLookup = FileLookupFactory.newInstance();
      InputStream is;
      if (localPathToSchema != null) {
         // Schema's are always stored in Infinispan
         is = fileLookup.lookupFile(localPathToSchema, null);
         if (is != null) {
            log.debugf("Using schema %s", localPathToSchema);
            return is;
         }
         if (log.isDebugEnabled()) {
            log.debugf("Could not find schema on path %s, resolving %s to %s",
                       localPathToSchema, SCHEMA_SYSTEM_PROPERTY, schemaPath());
         }
      }

      //2. resolve local schema path in infinispan distro
      is = fileLookup.lookupFile(schemaPath(), null);
      if (is != null) {
         log.debugf("Using schema %s", schemaPath());
         return is;
      }
      if (log.isDebugEnabled()) {
         log.debugf("Could not find schema on path %s, resolving %s to %s",
                    schemaPath(), SCHEMA_SYSTEM_PROPERTY, schemaURL());
      }

      //3. resolve URL
      try {
         is = new URL(schemaURL()).openStream();
         log.debugf("Using schema %s", schemaURL());
         return is;
      } catch (Exception e) {
      }

      log.couldNotResolveConfigurationSchema(localPathToSchema, schemaPath(), schemaURL());
      return null;
   }

   public static String resolveSchemaPath() {
      boolean validating = !skipSchemaValidation();
      if (!validating)
         return null;
      return schemaPath();
   }

   private static String schemaPath() {
      return SysPropertyActions.getProperty(SCHEMA_SYSTEM_PROPERTY, DEFAULT_SCHEMA_LOCATION);
   }

   private static String schemaURL() {
      return SysPropertyActions.getProperty(SCHEMA_URL_SYSTEM_PROPERTY, DEFAULT_SCHEMA_URL);
   }

   private InfinispanConfiguration() {
   }

   public void accept(ConfigurationBeanVisitor v) {
      if (v != null) {
         global.accept(v);
         defaultConfiguration.accept(v);
         for (Configuration c : namedCaches) {
            c.accept(v);
         }
         v.visitInfinispanConfiguration(this);
      }
   }

   private static InputStream findInputStream(String fileName, ClassLoader cl) throws FileNotFoundException {
      if (fileName == null)
         throw new NullPointerException("File name cannot be null!");
      FileLookup fileLookup = FileLookupFactory.newInstance();
      InputStream is = fileLookup.lookupFile(fileName, cl);
      if (is == null)
         throw new FileNotFoundException("File " + fileName
                 + " could not be found, either on the classpath or on the file system!");
      return is;
   }

   @Override
   public Configuration parseDefaultConfiguration() throws ConfigurationException {
      return defaultConfiguration;
   }

   @Override
   public GlobalConfiguration parseGlobalConfiguration() {
      return global;
   }

   @Override
   public Map<String, Configuration> parseNamedConfigurations() throws ConfigurationException {
      Map<String, Configuration> map = new HashMap<String, Configuration>(7);
      for (Configuration conf : namedCaches) {
         map.put(conf.getName(), conf);
      }
      return map;
   }

   @Override
   public void willUnmarshall(Object parent) {
      // no-op
   }

   /**
    * Converts this configuration instance to an XML representation containing the current settings.
    *
    * @return a string containing the formatted XML representation of this configuration instance.
    */
   public String toXmlString() {
      return toXmlString(this);
   }
}
