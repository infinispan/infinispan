package org.infinispan.config;

import org.infinispan.config.FluentConfiguration.GroupsConfig;
import org.infinispan.distribution.group.Grouper;
import org.infinispan.util.Util;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import java.util.AbstractList;
import java.util.LinkedList;
import java.util.List;

@XmlAccessorType(XmlAccessType.PROPERTY)
@ConfigurationDoc(name="groups",desc="Configuration for various grouper definitions. See the user guide for more information")
public class GroupsConfiguration extends AbstractFluentConfigurationBean implements GroupsConfig {
   
   
   List<Grouper<?>> groupers = new LinkedList<Grouper<?>>();
   
   @ConfigurationDocRef(targetElement = "enabled", bean = GroupsConfiguration.class)
   Boolean enabled = false;
   
   public void accept(ConfigurationBeanVisitor v) {
      v.visitGroupConfig(this);
   }
   
   @XmlElement(name = "grouper")
   public List<GrouperConfiguration> getGroupGeneratorConfigurations() {
      return new AbstractList<GrouperConfiguration>() {

         @Override
         public GrouperConfiguration get(int index) {
            return new GrouperConfiguration(groupers.get(index).getClass());
         }
         
         @Override
         public boolean add(GrouperConfiguration e) {
            return groupers.add(new LazyGrouper<Object>(e.getClazz()));
         }

         @Override
         public int size() {
            return groupers.size();
         }
         
         
      };
   }
   
   private class LazyGrouper<T> implements Grouper<T> {
      
      private final String className;
      private transient Grouper<T> delegate;
      
      public LazyGrouper(String className) {
         this.className = className;
      }

      @Override
      public String computeGroup(T key, String group) {
         return delegate().computeGroup(key, group);
      }
      
      @Override
      public Class<T> getKeyType() {
         return delegate().getKeyType();
      }
      
      private Grouper<T> delegate() {
         if (delegate == null)
            delegate = Util.getInstance(className, config.getClassLoader());
         return delegate;
      }
      
   }
   
   @Override
   public Boolean isEnabled() {
      return enabled;
   }
   
   @XmlAttribute(name = "enabled")
   public void setEnabled(Boolean enabled) {
      enabled(enabled);
   }

   /**
    * Enable grouping support
    * 
    */
   @Override
   public GroupsConfig enabled(Boolean enabled) {
      this.enabled = enabled;
      testImmutability("enabled");
      return this;
   }

   @Override
   public GroupsConfig groupers(List<Grouper<?>> groupers) {
      this.groupers = groupers;
      testImmutability("groupers");
      return this;
   }

   @Override
   public List<Grouper<?>> getGroupers() {
      return this.groupers;
   }
   
   public boolean equals(Object obj) {
      if (obj instanceof GroupsConfiguration) {
         GroupsConfiguration that = (GroupsConfiguration) obj;
         if (this.enabled != null && !this.enabled.equals(that.enabled))
            return false;
         else if (this.groupers != null && !this.groupers.equals(that.groupers))
            return false;
         else
            return true;
      } else
         return false;
   }
   
   public int hashCode() {
      int result = enabled != null ? enabled.hashCode() : 0;
      result = 31 * result + (groupers != null ? groupers.hashCode() : 0);
      return result;
   }
   
   @Override
   public GroupsConfiguration clone() throws CloneNotSupportedException {
       GroupsConfiguration dolly = (GroupsConfiguration) super.clone();
       dolly.enabled = enabled;
       dolly.groupers = new LinkedList<Grouper<?>>();
       for (Grouper<?> g : groupers) {
          dolly.groupers.add(g);
       }
       return dolly;
   }

}
