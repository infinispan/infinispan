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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

/**
 * Defines custom marshallable to be registered with marshalling framework
 *
 * @author Vladimir Blagojevic
 * @since 5.0
 */
@XmlAccessorType(XmlAccessType.PROPERTY)
@XmlType(name = "marshallable")
@ConfigurationDoc(name = "marshallable")
public class MarshallableConfig extends AbstractConfigurationBeanWithGCR {

   /** The serialVersionUID */
   private static final long serialVersionUID = -5161505617995274887L;

   @ConfigurationDocRef(bean = MarshallableConfig.class, targetElement = "setTypeClass")
   protected String typeClass;

   @ConfigurationDocRef(bean = MarshallableConfig.class, targetElement = "setExternalizerClass")
   protected String externalizerClass;

   @ConfigurationDocRef(bean = MarshallableConfig.class, targetElement = "setId")
   protected Integer id;

   public MarshallableConfig() {
      super();
   }

   public String getTypeClass() {
      return typeClass;
   }

   /**
    * Fully qualified name of the class that the configured {@link org.infinispan.marshall.Externalizer} can
    * marshall/unmarshall. Establishing the link between marshalled types and {@link org.infinispan.marshall.Externalizer}
    * implementations enables users to provide their own marshalling mechanisms even for classes which they cannot
    * modify or extend.
    * 
    * @param typeClass
    */
   @XmlAttribute
   public void setTypeClass(String typeClass) {
      this.typeClass = typeClass;
   }

   public String getExternalizerClass() {
      return externalizerClass;
   }

   /**
    * {@link org.infinispan.marshall.Externalizer} implementation that knows how
    * to marshall or unmarshall instances of a particular, user-defined, type.
    * 
    * @param externalizerClass
    */
   @XmlAttribute
   public void setExternalizerClass(String externalizerClass) {
      this.externalizerClass = externalizerClass;
   }

   public Integer getId() {
      return id;
   }

   /**
    * This identifier distinguishes between different user-defined classes, providing
    * a more performant way to ship class information around rather than passing
    * class names or class information in general around.
    *
    * Only positive ids are allowed, and you can use any number as long as it does not
    * clash with an already existing number for a different class. If there're any
    * clashes, Infinispan will abort startup and will provide class information of
    * the ids clashing.
    * 
    * @param id
    */
   @XmlAttribute
   public void setId(Integer id) {
      this.id = id;
   }

   public String toString() {
      return "MarshallableConfig{";
   }

   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (!(o instanceof MarshallableConfig))
         return false;

      MarshallableConfig that = (MarshallableConfig) o;
      if (typeClass != null && !typeClass.equals(that.typeClass))
         return false;
      if (externalizerClass != null && !externalizerClass.equals(that.externalizerClass))
         return false;
      if (id != null && !id.equals(that.id))
         return false;

      return true;
   }

   public int hashCode() {
      int result;
      result = (typeClass != null ? typeClass.hashCode() : 0);
      result = 31 * result + (externalizerClass != null ? externalizerClass.hashCode() : 0);
      result = 31 * result + (id != null ? id.hashCode() : 0);
      return result;
   }

   public void accept(ConfigurationBeanVisitor v) {
      v.visitMarshallableConfig(this);
   }
}
