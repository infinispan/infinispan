package org.infinispan.commons.configuration.io.json;

import java.io.BufferedReader;
import java.util.Properties;

import org.infinispan.commons.configuration.io.AbstractConfigurationReader;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolver;
import org.infinispan.commons.configuration.io.Location;
import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.configuration.io.PropertyReplacer;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 * TODO: this is currently a placeholder. To be implemented in ISPN-12722
 **/
public class JsonConfigurationReader extends AbstractConfigurationReader {

   public JsonConfigurationReader(BufferedReader reader, ConfigurationResourceResolver resourceResolver, Properties properties, PropertyReplacer replacer, NamingStrategy namingStrategy) {
      super(resourceResolver, properties, replacer, namingStrategy);
   }

   @Override
   public ElementType nextElement() {
      return null;
   }

   @Override
   public Location getLocation() {
      return null;
   }

   @Override
   public String getAttributeName(int index, NamingStrategy strategy) {
      return null;
   }

   @Override
   public String getLocalName(NamingStrategy strategy) {
      return null;
   }

   @Override
   public String getAttributeNamespace(int index) {
      return null;
   }

   @Override
   public String getAttributeValue(String localName) {
      return null;
   }

   @Override
   public String getAttributeValue(int index) {
      return null;
   }

   @Override
   public String getElementText() {
      return null;
   }

   @Override
   public String getNamespace() {
      return null;
   }

   @Override
   public boolean hasNext() {
      return false;
   }

   @Override
   public int getAttributeCount() {
      return 0;
   }

   @Override
   public void require(ElementType type, String namespace, String name) {
   }

   @Override
   public void close() throws Exception {
   }
}
