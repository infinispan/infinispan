package org.infinispan.server.configuration.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class SSLConfiguration implements ConfigurationInfo {

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SSL.toString());

   private final KeyStoreConfiguration keyStore;
   private final TrustStoreConfiguration trustStore;
   private final SSLEngineConfiguration engine;
   private final List<ConfigurationInfo> subElements = new ArrayList<>();

   SSLConfiguration(KeyStoreConfiguration keyStore, TrustStoreConfiguration trustStore, SSLEngineConfiguration engine) {
      this.keyStore = keyStore;
      this.trustStore = trustStore;
      this.engine = engine;
      this.subElements.addAll(Arrays.asList(keyStore, engine));
   }

   public KeyStoreConfiguration keyStore() {
      return keyStore;
   }

   public TrustStoreConfiguration trustStore() {
      return trustStore;
   }

   SSLEngineConfiguration engine() {
      return engine;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

}
