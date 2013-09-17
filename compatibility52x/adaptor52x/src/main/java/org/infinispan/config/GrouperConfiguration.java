package org.infinispan.config;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

@XmlAccessorType(XmlAccessType.PROPERTY)
@ConfigurationDoc(name="grouper",desc="Grouper definition. See The Grouping API for more details")
public class GrouperConfiguration extends AbstractNamedCacheConfigurationBean{

   @ConfigurationDocRef(name = "class", targetElement = "setClazz", bean = GrouperConfiguration.class)
   String clazz;
   
   public GrouperConfiguration() {
   }
   
   GrouperConfiguration(Class<?> clazz) {
      this.clazz = clazz.getName();
   }
   
   public String getClazz() {
      return clazz;
   }
   
   /**
    * The grouper class
    * 
    * @param clazz
    */
   @XmlAttribute(name="class")   
   public void setClazz(String clazz) {
      this.clazz = clazz;
   }
}