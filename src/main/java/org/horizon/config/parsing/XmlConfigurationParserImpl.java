package org.horizon.config.parsing;

import org.horizon.config.CacheLoaderManagerConfig;
import org.horizon.config.Configuration;
import org.horizon.config.ConfigurationException;
import org.horizon.config.CustomInterceptorConfig;
import org.horizon.config.DuplicateCacheNameException;
import org.horizon.config.GlobalConfiguration;
import org.horizon.config.parsing.element.CustomInterceptorsElementParser;
import org.horizon.config.parsing.element.LoadersElementParser;
import org.horizon.lock.IsolationLevel;
import org.horizon.transaction.GenericTransactionManagerLookup;
import org.horizon.util.FileLookup;
import org.horizon.eviction.EvictionStrategy;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * The default XML configuration parser
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class XmlConfigurationParserImpl extends XmlParserBase implements XmlConfigurationParser {

   // this parser will need to be initialized.
   boolean initialized = false;

   // the root element, representing the <horizon /> tag
   Element rootElement;

   GlobalConfiguration gc;
   Map<String, Configuration> namedCaches;

   /**
    * Constructs a new parser
    */
   public XmlConfigurationParserImpl() {
   }

   /**
    * Constructs a parser and initializes it with the file name passed in, by calling {@link #initialize(String)}.
    *
    * @param fileName file name to initialize the parser with
    * @throws IOException if there is a problem reading or locating the file.
    */
   public XmlConfigurationParserImpl(String fileName) throws IOException {
      initialize(fileName);
   }

   /**
    * Constructs a parser and initializes it with the input stream passed in, by calling {@link
    * #initialize(InputStream)}.
    *
    * @param inputStream input stream to initialize the parser with
    * @throws IOException if there is a problem reading the stream
    */
   public XmlConfigurationParserImpl(InputStream inputStream) throws IOException {
      initialize(inputStream);
   }

   public void initialize(String fileName) throws IOException {
      if (fileName == null) throw new NullPointerException("File name cannot be null!");
      FileLookup fileLookup = new FileLookup();
      InputStream is = fileLookup.lookupFile(fileName);
      if (is == null)
         throw new FileNotFoundException("File " + fileName + " could not be found, either on the classpath or on the file system!");
      initialize(is);
   }

   public void initialize(InputStream inputStream) throws IOException {
      if (inputStream == null) throw new NullPointerException("Input stream cannot be null!");
      initialized = true;
      rootElement = new RootElementBuilder().readRoot(inputStream);
   }

   public Configuration parseDefaultConfiguration() throws ConfigurationException {
      assertInitialized();
      if (gc == null) {
         Element defaultElement = getSingleElementInCoreNS("default", rootElement);
         // there may not be a <default /> element!
         if (defaultElement == null) {
            return new Configuration();
         } else {
            defaultElement.normalize();
            return parseConfiguration(defaultElement);
         }
      } else {
         return gc.getDefaultConfiguration();
      }
   }

   public Map<String, Configuration> parseNamedConfigurations() throws ConfigurationException {
      assertInitialized();
      // there may not be any namedCache elements!
      if (namedCaches == null) {
         Set<Element> elements = getAllElementsInCoreNS("namedCache", rootElement);
         if (elements.isEmpty()) return Collections.emptyMap();
         namedCaches = new HashMap<String, Configuration>(elements.size(), 1.0f);
         for (Element e : elements) {
            String configurationName = getAttributeValue(e, "name");
            if (namedCaches.containsKey(configurationName)) {
               namedCaches = null;
               throw new DuplicateCacheNameException("Named cache " + configurationName + " is declared more than once!");
            }
            namedCaches.put(configurationName, parseConfiguration(e));
         }
      }

      return namedCaches;
   }

   public GlobalConfiguration parseGlobalConfiguration() {
      assertInitialized();
      if (gc == null) {
         Element globalElement = getSingleElementInCoreNS("global", rootElement);
         Configuration defaultConfig = parseDefaultConfiguration();
         gc = new GlobalConfiguration();
         gc.setDefaultConfiguration(defaultConfig);
         // there may not be a <global /> element in the config!!
         if (globalElement != null) {
            globalElement.normalize();
            configureAsyncListenerExecutor(getSingleElementInCoreNS("asyncListenerExecutor", globalElement), gc);
            configureAsyncSerializationExecutor(getSingleElementInCoreNS("asyncSerializationExecutor", globalElement), gc);
            configureEvictionScheduledExecutor(getSingleElementInCoreNS("evictionScheduledExecutor", globalElement), gc);
            configureReplicationQueueScheduledExecutor(getSingleElementInCoreNS("replicationQueueScheduledExecutor", globalElement), gc);
            configureTransport(getSingleElementInCoreNS("transport", globalElement), gc);
            configureShutdown(getSingleElementInCoreNS("shutdown", globalElement), gc);
            configureSerialization(getSingleElementInCoreNS("serialization", globalElement), gc);
         }
      }

      return gc;
   }

   private Configuration parseConfiguration(Element e) {
      Configuration c = new Configuration();
      configureLocking(getSingleElementInCoreNS("locking", e), c);
      configureTransaction(getSingleElementInCoreNS("transaction", e), c);
      configureJmxStatistics(getSingleElementInCoreNS("jmxStatistics", e), c);
      configureLazyDeserialization(getSingleElementInCoreNS("lazyDeserialization", e), c);
      configureInvocationBatching(getSingleElementInCoreNS("invocationBatching", e), c);
      configureClustering(getSingleElementInCoreNS("clustering", e), c);
      configureEviction(getSingleElementInCoreNS("eviction", e), c);
      configureCacheLoaders(getSingleElementInCoreNS("loaders", e), c);
      configureCustomInterceptors(getSingleElementInCoreNS("customInterceptors", e), c);

      return c;
   }

   private void assertInitialized() {
      if (!initialized)
         throw new ConfigurationException("Parser not initialized.  Please invoke initialize() first, or use a constructor that initializes the parser.");
   }

   void configureClustering(Element e, Configuration config) {
      if (e == null) return; //we might not have this configured
      // there are 2 attribs - mode and clusterName
      boolean repl = true;
      String mode = getAttributeValue(e, "mode").toUpperCase();
      if (mode.startsWith("R"))
         repl = true;
      else if (mode.startsWith("I"))
         repl = false;

      Element asyncEl = getSingleElementInCoreNS("async", e);
      Element syncEl = getSingleElementInCoreNS("sync", e);
      if (syncEl != null && asyncEl != null)
         throw new ConfigurationException("Cannot have sync and async elements within the same cluster element!");
      boolean sync = asyncEl == null; // even if both are null, we default to sync
      if (sync) {
         config.setCacheMode(repl ? Configuration.CacheMode.REPL_SYNC : Configuration.CacheMode.INVALIDATION_SYNC);
         configureSyncMode(syncEl, config);
      } else {
         config.setCacheMode(repl ? Configuration.CacheMode.REPL_ASYNC : Configuration.CacheMode.INVALIDATION_ASYNC);
         configureAsyncMode(asyncEl, config);
      }
      String cn = getAttributeValue(e, "clusterName");
//      if (existsAttribute(cn)) config.setClusterName(cn);
      configureStateRetrieval(getSingleElementInCoreNS("stateRetrieval", e), config);
//      configureTransport(getSingleElementInCoreNS("jgroupsConfig", e), config);
   }

   void configureStateRetrieval(Element element, Configuration config) {
      if (element == null) return; //we might not have this configured
      String fetchInMemoryState = getAttributeValue(element, "fetchInMemoryState");
      if (existsAttribute(fetchInMemoryState)) config.setFetchInMemoryState(getBoolean(fetchInMemoryState));
      String stateRetrievalTimeout = getAttributeValue(element, "timeout");
      if (existsAttribute(stateRetrievalTimeout)) config.setStateRetrievalTimeout(getLong(stateRetrievalTimeout));

   }

   void configureTransaction(Element element, Configuration config) {
      if (element != null) {
         String tmp = getAttributeValue(element, "transactionManagerLookupClass");
         if (existsAttribute(tmp)) {
            config.setTransactionManagerLookupClass(tmp);
         } else {
            // use defaults since the transaction element is still present!
            config.setTransactionManagerLookupClass(GenericTransactionManagerLookup.class.getName());
         }
         String syncRollbackPhase = getAttributeValue(element, "syncRollbackPhase");
         if (existsAttribute(syncRollbackPhase)) config.setSyncRollbackPhase(getBoolean(syncRollbackPhase));
         String syncCommitPhase = getAttributeValue(element, "syncCommitPhase");
         if (existsAttribute(syncCommitPhase)) config.setSyncCommitPhase(getBoolean(syncCommitPhase));
      }
   }

   void configureCustomInterceptors(Element element, Configuration config) {
      if (element == null) return; //this element might be missing
      CustomInterceptorsElementParser parser = new CustomInterceptorsElementParser();
      List<CustomInterceptorConfig> interceptorConfigList = parser.parseCustomInterceptors(element);
      config.setCustomInterceptors(interceptorConfigList);
   }

   void configureInvocationBatching(Element element, Configuration config) {
      if (element != null) {
         String enabled = getAttributeValue(element, "enabled");
         if (existsAttribute(enabled)) {
            config.setInvocationBatchingEnabled(getBoolean(enabled));
         } else {
            // enable this anyway since the XML element is present
            config.setInvocationBatchingEnabled(true);
         }

      }
   }

   void configureCacheLoaders(Element element, Configuration config) {
      if (element == null) return; //null cache loaders are allowed
      LoadersElementParser clElementParser = new LoadersElementParser();
      CacheLoaderManagerConfig cacheLoaderConfig = clElementParser.parseLoadersElement(element);
      config.setCacheLoaderManagerConfig(cacheLoaderConfig);
   }

   void configureExpiration(Element expirationElement, Configuration config) {
      if (expirationElement != null) {
         String tmp = getAttributeValue(expirationElement, "lifespan");
         if (existsAttribute(tmp)) config.setExpirationLifespan(getLong(tmp));
         tmp = getAttributeValue(expirationElement, "maxIdle");
         if (existsAttribute(tmp)) config.setExpirationMaxIdle(getLong(tmp));
      }
   }

   void configureEviction(Element evictionElement, Configuration config) {
      if (evictionElement != null) {
         String tmp = getAttributeValue(evictionElement, "strategy");
         if (existsAttribute(tmp)) config.setEvictionStrategy(EvictionStrategy.valueOf(tmp.trim().toUpperCase()));
         tmp = getAttributeValue(evictionElement, "maxEntries");
         if (existsAttribute(tmp)) config.setEvictionMaxEntries(getInt(tmp));
         tmp = getAttributeValue(evictionElement, "wakeUpInterval");
         if (existsAttribute(tmp)) config.setEvictionWakeupInterval(getLong(tmp));
      }
   }

   void configureJmxStatistics(Element element, Configuration config) {
      if (element != null) {
         String enabled = getAttributeValue(element, "enabled");
         if (existsAttribute(enabled)) {
            config.setExposeJmxStatistics(getBoolean(enabled));
         } else {
            // by default enable this since the element is present!
            config.setExposeJmxStatistics(true);
         }
         String jmxNameBase = getAttributeValue(element, "jmxNameBase");
         if (existsAttribute(jmxNameBase)) {
            //todo update this
//            config.setJmxNameBase(jmxNameBase);
         }
      }
   }

   void configureLazyDeserialization(Element element, Configuration config) {
      if (element != null) {
         String enabled = getAttributeValue(element, "enabled");
         if (existsAttribute(enabled)) {
            config.setUseLazyDeserialization(getBoolean(enabled));
         }
      }
   }

   void configureInvalidation(Element element, Configuration config) {
      if (element == null) return; //might be replication
      Element async = getSingleElement("async");
      if (async != null) {
         config.setCacheMode(Configuration.CacheMode.INVALIDATION_ASYNC);
         configureAsyncMode(getSingleElementInCoreNS("async", element), config);
      }
      Element sync = getSingleElement("sync");
      if (sync != null) {
         config.setCacheMode(Configuration.CacheMode.INVALIDATION_SYNC);
         configureSyncMode(getSingleElementInCoreNS("sync", element), config);
      }
   }

   void configureSyncMode(Element element, Configuration config) {
      String replTimeout = getAttributeValue(element, "replTimeout");
      if (existsAttribute(replTimeout)) config.setSyncReplTimeout(getLong(replTimeout));
   }

   void configureAsyncMode(Element element, Configuration config) {
      String tmp = getAttributeValue(element, "useReplQueue");
      if (existsAttribute(tmp)) config.setUseReplQueue(getBoolean(tmp));
      tmp = getAttributeValue(element, "replQueueInterval");
      if (existsAttribute(tmp)) config.setReplQueueInterval(getLong(tmp));
      tmp = getAttributeValue(element, "replQueueMaxElements");
      if (existsAttribute(tmp)) config.setReplQueueMaxElements(getInt(tmp));
      tmp = getAttributeValue(element, "useAsyncSerialization");
      if (existsAttribute(tmp)) config.setUseAsyncSerialization(getBoolean(tmp));
   }

   void configureLocking(Element element, Configuration config) {
      String isolationLevel = getAttributeValue(element, "isolationLevel");
      if (existsAttribute(isolationLevel)) config.setIsolationLevel(IsolationLevel.valueOf(isolationLevel));
      String lockAcquisitionTimeout = getAttributeValue(element, "lockAcquisitionTimeout");
      if (existsAttribute(lockAcquisitionTimeout)) config.setLockAcquisitionTimeout(getLong(lockAcquisitionTimeout));
      String writeSkewCheck = getAttributeValue(element, "writeSkewCheck");
      if (existsAttribute(writeSkewCheck)) config.setWriteSkewCheck(getBoolean(writeSkewCheck));
      String useLockStriping = getAttributeValue(element, "useLockStriping");
      if (existsAttribute(useLockStriping)) config.setUseLockStriping(getBoolean(useLockStriping));
      String concurrencyLevel = getAttributeValue(element, "concurrencyLevel");
      if (existsAttribute(concurrencyLevel)) config.setConcurrencyLevel(getInt(concurrencyLevel));
   }

   // ----------------------------------------------------------------------------------------------------------------
   //      Configure the GlobalConfiguration object
   // ----------------------------------------------------------------------------------------------------------------

   void configureShutdown(Element element, GlobalConfiguration config) {
      if (element != null) {
         String hookBehavior = getAttributeValue(element, "hookBehavior");
         if (existsAttribute(hookBehavior)) config.setShutdownHookBehavior(hookBehavior);
      }
   }

   void configureTransport(Element e, GlobalConfiguration gc) {
      // if the element does NOT exist then don't use a transport class at all!
      if (e != null) {
         String tmp = getAttributeValue(e, "transportClass");
         if (existsAttribute(tmp)) {
            gc.setTransportClass(tmp);
         } else {
            // the class is not specified; use the default
            gc.setTransportClass(GlobalConfiguration.getClusteredDefault().getTransportClass());
         }

         tmp = getAttributeValue(e, "clusterName");
         if (existsAttribute(tmp)) gc.setClusterName(tmp);

         tmp = getAttributeValue(e, "distributedSyncTimeout");
         if (existsAttribute(tmp)) gc.setDistributedSyncTimeout(getLong(tmp));

         Properties p = XmlConfigHelper.extractProperties(e);
         if (p != null) gc.setTransportProperties(p);
      }
   }

   void configureSerialization(Element e, GlobalConfiguration configuration) {
      if (e != null) {
         String tmp = getAttributeValue(e, "marshallerClass");
         if (existsAttribute(tmp)) configuration.setMarshallerClass(tmp);

         tmp = getAttributeValue(e, "version");
         if (existsAttribute(tmp)) configuration.setMarshallVersion(tmp);

         tmp = getAttributeValue(e, "objectInputStreamPoolSize");
         if (existsAttribute(tmp)) configuration.setObjectInputStreamPoolSize(getInt(tmp));

         tmp = getAttributeValue(e, "objectOutputStreamPoolSize");
         if (existsAttribute(tmp)) configuration.setObjectOutputStreamPoolSize(getInt(tmp));
      }
   }

   void configureAsyncListenerExecutor(Element e, GlobalConfiguration gc) {
      if (e != null) {
         String tmp = getAttributeValue(e, "factory");
         if (existsAttribute(tmp)) gc.setAsyncListenerExecutorFactoryClass(tmp);
         Properties p = XmlConfigHelper.extractProperties(e);
         if (p != null) gc.setAsyncListenerExecutorProperties(p);
      }
   }

   void configureAsyncSerializationExecutor(Element e, GlobalConfiguration gc) {
      if (e != null) {
         String tmp = getAttributeValue(e, "factory");
         if (existsAttribute(tmp)) gc.setAsyncSerializationExecutorFactoryClass(tmp);
         Properties p = XmlConfigHelper.extractProperties(e);
         if (p != null) gc.setAsyncSerializationExecutorProperties(p);
      }
   }

   void configureEvictionScheduledExecutor(Element e, GlobalConfiguration gc) {
      if (e != null) {
         String tmp = getAttributeValue(e, "factory");
         if (existsAttribute(tmp)) gc.setEvictionScheduledExecutorFactoryClass(tmp);
         Properties p = XmlConfigHelper.extractProperties(e);
         if (p != null) gc.setEvictionScheduledExecutorProperties(p);
      }
   }

   void configureReplicationQueueScheduledExecutor(Element e, GlobalConfiguration gc) {
      if (e != null) {
         String tmp = getAttributeValue(e, "factory");
         if (existsAttribute(tmp)) gc.setReplicationQueueScheduledExecutorFactoryClass(tmp);
         Properties p = XmlConfigHelper.extractProperties(e);
         if (p != null) gc.setReplicationQueueScheduledExecutorProperties(p);
      }
   }

   private Element getSingleElement(String elementName) {
      return getSingleElementInCoreNS(elementName, rootElement);
   }

   /**
    * Tests whether the element passed in is a valid config element.
    *
    * @param element element to test
    * @return true of the element is a modern one and can be parsed using the current parser.
    */
   public boolean isValidElementRoot(Element element) {
      // simply test for the "horizon" element.
      NodeList elements = element.getElementsByTagName("horizon");
      return elements != null && elements.getLength() > 0;
   }
}
