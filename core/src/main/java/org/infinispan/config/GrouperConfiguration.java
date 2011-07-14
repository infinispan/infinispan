package org.infinispan.config;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

@XmlAccessorType(XmlAccessType.PROPERTY)
public class GrouperConfiguration {

   String clazz;
   
   public GrouperConfiguration() {
      // TODO Auto-generated constructor stub
   }
   
   GrouperConfiguration(Class<?> clazz) {
      this.clazz = clazz.getName();
   }
   
   public String getClazz() {
      return clazz;
   }
   
   @XmlAttribute(name="class")
   public void setClazz(String clazz) {
      this.clazz = clazz;
   }
}