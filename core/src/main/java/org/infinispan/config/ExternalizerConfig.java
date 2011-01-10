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

import org.infinispan.marshall.Externalizer;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

/**
 * Defines custom Externalizers to be registered with marshalling framework
 *
 * @author Vladimir Blagojevic
 * @since 5.0
 */
@XmlAccessorType(XmlAccessType.PROPERTY)
@XmlType(name = "marshallable")
@ConfigurationDoc(name = "marshallable")
public class ExternalizerConfig extends AbstractConfigurationBeanWithGCR {

   /** The serialVersionUID */
   private static final long serialVersionUID = -5161505617995274887L;

   @ConfigurationDocRef(bean = ExternalizerConfig.class, targetElement = "setExternalizerClass")
   protected String externalizerClass;

   private Externalizer externalizer;

   @ConfigurationDocRef(bean = ExternalizerConfig.class, targetElement = "setId")
   protected Integer id;

   public String getExternalizerClass() {
      if (externalizerClass == null && externalizer != null)
         externalizerClass = externalizer.getClass().getName();

      return externalizerClass;
   }

   /**
    * Fully qualified class name of an {@link org.infinispan.marshall.Externalizer}
    * implementation that knows how to marshall or unmarshall instances of one, or
    * several, user-defined, types.
    * 
    * @param externalizerClass
    */
   @XmlAttribute
   public ExternalizerConfig setExternalizerClass(String externalizerClass) {
      this.externalizerClass = externalizerClass;
      return this;
   }

   public Integer getId() {
      if (id == null && externalizer != null)
         id = externalizer.getId();
      return id;
   }

   /**
    * This identifier distinguishes between different user-defined {@link Externalizer}
    * implementations, providing a more performant way to ship class information around
    * rather than passing class names or class information in general around.
    *
    * Only positive ids are allowed, and you can use any number as long as it does not
    * clash with an already existing number for a {@link Externalizer} implementation.
    *
    * If there're any clashes, Infinispan will abort startup and will provide class
    * information of the ids clashing.
    * 
    * @param id
    */
   @XmlAttribute
   public ExternalizerConfig setId(Integer id) {
      this.id = id;
      return this;
   }

   @XmlTransient // Prevent JAXB from thinking that externalizer is an XML attribute
   public Externalizer getExternalizer() {
      return externalizer;
   }

   public ExternalizerConfig setExternalizer(Externalizer externalizer) {
      this.externalizer = externalizer;
      return this;
   }

   public String toString() {
      return "ExternalizerConfig{";
   }

   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (!(o instanceof ExternalizerConfig))
         return false;

      ExternalizerConfig that = (ExternalizerConfig) o;
      if (externalizerClass != null && !externalizerClass.equals(that.externalizerClass))
         return false;
      if (id != null && !id.equals(that.id))
         return false;

      return true;
   }

   public int hashCode() {
      int result;
      result = (externalizerClass != null ? externalizerClass.hashCode() : 0);
      result = 31 * result + (id != null ? id.hashCode() : 0);
      return result;
   }

   public void accept(ConfigurationBeanVisitor v) {
      v.visitExternalizerConfig(this);
   }
}
