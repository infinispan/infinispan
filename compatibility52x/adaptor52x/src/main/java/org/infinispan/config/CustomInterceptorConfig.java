package org.infinispan.config;

import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.TypedProperties;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import java.util.Locale;
import java.util.Properties;

/**
 * Holds information about the custom interceptors defined in the configuration file.
 *
 *
 * @author Mircea.Markus@jboss.com
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@XmlAccessorType(XmlAccessType.PROPERTY)
@XmlType(name="interceptor")
@ConfigurationDoc(name="interceptor")
public class CustomInterceptorConfig extends AbstractNamedCacheConfigurationBean {

   /** The serialVersionUID */
   private static final long serialVersionUID = 6206233611032238190L;

   @XmlTransient
   protected CommandInterceptor interceptor;

   @ConfigurationDocRef(name="class", bean=CustomInterceptorConfig.class,targetElement="setIndex")
   protected Integer index = -1;
   
   @ConfigurationDocRef(name="class", bean=CustomInterceptorConfig.class,targetElement="setAfterInterceptor")
   protected String after;

   @ConfigurationDocRef(name="class", bean=CustomInterceptorConfig.class,targetElement="setBeforeInterceptor")
   protected String before;

   @ConfigurationDocRef(name="class", bean=CustomInterceptorConfig.class,targetElement="setPosition")
   protected Position position = Position.OTHER_THAN_FIRST_OR_LAST;   
      
   @ConfigurationDocRef(name="class", bean=CustomInterceptorConfig.class,targetElement="setClassName")
   protected String className;

   @XmlElement
   private TypedProperties properties = new TypedProperties();

   public CustomInterceptorConfig() {
      super();
      overriddenConfigurationElements.add("isFirst");
   }

   /**
    * Builds a custom interceptor configuration.
    *
    * @param interceptor interceptor instance, already initialized with all attributes specified in the configuration
    * @param first       true if you wan this to be the first interceptor in the chain
    * @param last        true if you wan this to be the last interceptor in the chain
    * @param index       an absolute position within the interceptor chain
    * @param after       if you want this interceptor immediately after the specified class in the chain
    * @param before      immediately before the specified class in the chain
    */
   public CustomInterceptorConfig(CommandInterceptor interceptor, boolean first, boolean last, int index,
                                  String after, String before) {
      this.interceptor = interceptor;     
      this.index = index;
      this.after = after;
      this.before = before;
      if (interceptor != null) overriddenConfigurationElements.add("interceptor");

      if (first && last)
         throw new IllegalArgumentException("Interceptor " + interceptor
                  + " can not be both first and last!");
      if (first) {
         position = Position.FIRST;
      }
      if (last) {
         position = Position.LAST;
      }
      // No way to tell here, unfortunately...
      overriddenConfigurationElements.add("isFirst");
      overriddenConfigurationElements.add("isLast");

      if (index > -1) overriddenConfigurationElements.add("index");

      if (after != null && after.length() > 0) overriddenConfigurationElements.add("after");
      if (before != null && before.length() > 0) overriddenConfigurationElements.add("before");
   }


   /**
    * Constructs an interceptor config based on the supplied interceptor instance.
    *
    * @param interceptor
    */
   public CustomInterceptorConfig(CommandInterceptor interceptor) {
      this();
      this.interceptor = interceptor;
      overriddenConfigurationElements.add("interceptor");
   }
   
   public Properties getProperties() {
      return properties;
   }
   
   @XmlTransient
   public void setProperties(Properties properties) {
      this.properties = toTypedProperties(properties);
      testImmutability("properties");
   }

   public Position getPosition() {
      return position;
   }
   
   public String getPositionAsString() {
      return position.toString();
   }

   /**
    * A position at which to place this interceptor in the chain. FIRST is the first interceptor
    * encountered when an invocation is made on the cache, LAST is the last interceptor before the
    * call is passed on to the data structure. Note that this attribute is mutually exclusive with
    * 'before', 'after' and 'index'.
    * 
    * @param position
    */
   @XmlTransient
   public void setPosition(Position position) {
      this.position = position;
      testImmutability("position");
   }

   public String getClassName() {
      return className;
   }

   /**
    * Fully qualified interceptor class name which must extend org.infinispan.interceptors.base.CommandInterceptor.
    * @param className
    */
   @XmlAttribute(name="class")
   public void setClassName(String className) {
      this.className = className;
      testImmutability("className");
   }

   /**
    * Shall this interceptor be the first one in the chain?
    */
   @XmlTransient
   public void setFirst(boolean first) {
      testImmutability("first");
      setPosition(Position.FIRST);
   }

   /**
    * Shall this interceptor be the last one in the chain?
    */
   @XmlTransient
   public void setLast(boolean last) {
      testImmutability("last");
      setPosition(Position.LAST);
   }
   
   /**
    * A position at which to place this interceptor in the chain. FIRST is the first interceptor
    * encountered when an invocation is made on the cache, LAST is the last interceptor before the
    * call is passed on to the data structure. Note that this attribute is mutually exclusive with
    * 'before', 'after' and 'index'.
    * 
    * @param pos
    */
   @XmlAttribute(name="position")
   public void setPositionAsString(String pos) {
      setPosition(Position.valueOf(uc(pos)));
   }

   
   /**
    * A position at which to place this interceptor in the chain, with 0 being the first position.
    * Note that this attribute is mutually exclusive with 'position', 'before' and 'after'."
    * 
    * @param index
    */
   @XmlAttribute(name="index")
   public void setIndex(Integer index) {
      testImmutability("index");
      this.index = index;
   }

   
   /**
    * Places the new interceptor directly after the instance of the named interceptor which is
    * specified via its fully qualified class name. Note that this attribute is mutually exclusive
    * with 'position', 'before' and 'index'.
    * 
    * @param afterClass
    */
   @XmlAttribute(name="after")
   public void setAfterInterceptor(String afterClass) {
      testImmutability("after");
      this.after = afterClass;
   }

   /**
    * Places the new interceptor directly after the instance of the named interceptor which is
    * specified via its fully qualified class name. Note that this attribute is mutually exclusive
    * with 'position', 'before' and 'index'.
    * 
    * @param interceptorClass
    */
   @XmlTransient
   public void setAfterInterceptor(Class<? extends CommandInterceptor> interceptorClass) {
      setAfterInterceptor(interceptorClass.getName());
   }

   /**
    * Places the new interceptor directly before the instance of the named interceptor which is
    * specified via its fully qualified class name.. Note that this attribute is mutually exclusive
    * with 'position', 'after' and 'index'."
    * 
    * @param beforeClass
    */
   @XmlAttribute(name="before")
   public void setBeforeInterceptor(String beforeClass) {
      testImmutability("before");
      this.before = beforeClass;
   }

   /**
    * Places the new interceptor directly before the instance of the named interceptor which is
    * specified via its fully qualified class name.. Note that this attribute is mutually exclusive
    * with 'position', 'after' and 'index'."
    * 
    * @param interceptorClass
    */
   @XmlTransient
   public void setBeforeInterceptor(Class<? extends CommandInterceptor> interceptorClass) {
      setBeforeInterceptor(interceptorClass.getName());
   }

   /**
    * Returns a the interceptor that we want to add to the chain.
    */
   public CommandInterceptor getInterceptor() {
      return interceptor;
   }
   
   /**
    * Returns a the interceptor that we want to add to the chain.
    */
   @XmlTransient
   public void setInterceptor(CommandInterceptor interceptor) {
      testImmutability("interceptor");
      this.interceptor = interceptor;
   }

   /**
    * @see #setFirst(boolean)
    */
   public boolean isFirst() {
      return getPosition() == Position.FIRST;
   }

   /**
    * @see #setLast(boolean)
    */
   public boolean isLast() {
      return getPosition() == Position.LAST;
   }

   /**
    * @see #getIndex()
    */
   public Integer getIndex() {
      return index;
   }

   /**
    * @see #getAfter()
    */
   public String getAfter() {
      return after;
   }
   
   /**
    * @see #getAfter()
    */
   public String getAfterInterceptor() {
      //DO NOT remove this method as it is needed for proper marshalling
      return getAfter();
   }

   /**
    * @see #getBefore()
    */
   public String getBefore() {
      return before;
   }
   
   public String getBeforeInterceptor() {
      //DO NOT remove this method as it is needed for proper marshalling
      return getBefore();
   }

   public String toString() {
      return "CustomInterceptorConfig{" +
            "interceptor='" + interceptor + '\'' +
            ", isFirst=" + isFirst() +
            ", isLast=" + isLast() +
            ", index=" + index +
            ", after='" + after + '\'' +
            ", before='" + before + '\'' +
            ", position='" + position + '\'' +
            ", class='" + className + '\'' +
            '}';
   }

   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof CustomInterceptorConfig)) return false;

      CustomInterceptorConfig that = (CustomInterceptorConfig) o;

      if (index != null && !index.equals(that.index)) return false;
      if (after != null ? !after.equals(that.after) : that.after != null) return false;
      if (before != null ? !before.equals(that.before) : that.before != null) return false;
      if (position != null ? !position.equals(that.position) : that.position != null) return false;
      if (interceptor != null ? !interceptor.equals(that.interceptor) : that.interceptor != null)
         return false;
      return true;
   }

   public int hashCode() {
      int result;
      result = (interceptor != null ? interceptor.hashCode() : 0);
      result = 31 * result + index;
      result = 31 * result + (after != null ? after.hashCode() : 0);
      result = 31 * result + (before != null ? before.hashCode() : 0);
      result = 31 * result + (position != null ? position.hashCode() : 0);
      return result;
   }

   @Override
   public CustomInterceptorConfig clone() throws CloneNotSupportedException {
      CustomInterceptorConfig dolly = (CustomInterceptorConfig) super.clone();
      if (properties != null) dolly.properties = (TypedProperties) properties.clone();
      return dolly;
   }
   
   @Override
   protected String uc(String s) {
      return s == null ? null : s.toUpperCase(Locale.ENGLISH);
   }
   
   enum Position {
      FIRST,LAST, OTHER_THAN_FIRST_OR_LAST
   }

   public void accept(ConfigurationBeanVisitor v) {
      v.visitCustomInterceptorConfig(this);
   }
}
