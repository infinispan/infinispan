package org.infinispan.config;

import org.infinispan.marshall.AdvancedExternalizer;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;

/**
 * Defines custom Externalizers to be registered with marshalling framework
 *
 * @author Vladimir Blagojevic
 * @since 5.0
 */
@XmlAccessorType(XmlAccessType.PROPERTY)
@ConfigurationDoc(name = "advancedExternalizer")
public class AdvancedExternalizerConfig extends AbstractConfigurationBeanWithGCR {

   /** The serialVersionUID */
   private static final long serialVersionUID = -5161505617995274887L;

   @ConfigurationDocRef(bean = AdvancedExternalizerConfig.class, targetElement = "setExternalizerClass")
   protected String externalizerClass;

   private AdvancedExternalizer<?> advancedExternalizer;

   @ConfigurationDocRef(bean = AdvancedExternalizerConfig.class, targetElement = "setId")
   protected Integer id;

   public String getExternalizerClass() {
      if (externalizerClass == null && advancedExternalizer != null)
         externalizerClass = advancedExternalizer.getClass().getName();

      return externalizerClass;
   }

   /**
    * Fully qualified class name of an {@link org.infinispan.marshall.AdvancedExternalizer}
    * implementation that knows how to marshall or unmarshall instances of one, or
    * several, user-defined, types.
    * 
    * @param externalizerClass
    */
   @XmlAttribute
   public AdvancedExternalizerConfig setExternalizerClass(String externalizerClass) {
      this.externalizerClass = externalizerClass;
      return this;
   }

   public Integer getId() {
      if (id == null && advancedExternalizer != null)
         id = advancedExternalizer.getId();
      return id;
   }

   /**
    * This identifier distinguishes between different user-defined {@link org.infinispan.marshall.AdvancedExternalizer}
    * implementations, providing a more performant way to ship class information around
    * rather than passing class names or class information in general around.
    *
    * Only positive ids are allowed, and you can use any number as long as it does not
    * clash with an already existing number for a {@link org.infinispan.marshall.AdvancedExternalizer} implementation.
    *
    * If there're any clashes, Infinispan will abort startup and will provide class
    * information of the ids clashing.
    * 
    * @param id
    */
   @XmlAttribute
   public AdvancedExternalizerConfig setId(Integer id) {
      this.id = id;
      return this;
   }

   @XmlTransient // Prevent JAXB from thinking that advancedExternalizer is an XML attribute
   public AdvancedExternalizer<?> getAdvancedExternalizer() {
      return advancedExternalizer;
   }

   public AdvancedExternalizerConfig setAdvancedExternalizer(AdvancedExternalizer<?> advancedExternalizer) {
      this.advancedExternalizer = advancedExternalizer;
      return this;
   }

   public String toString() {
      return "AdvancedExternalizerConfig{";
   }

   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (!(o instanceof AdvancedExternalizerConfig))
         return false;

      AdvancedExternalizerConfig that = (AdvancedExternalizerConfig) o;
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
      v.visitAdvancedExternalizerConfig(this);
   }
}
