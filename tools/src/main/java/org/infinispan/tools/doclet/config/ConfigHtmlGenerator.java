package org.infinispan.tools.doclet.config;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import org.infinispan.config.AbstractConfigurationBean;
import org.infinispan.config.ConfigurationAttribute;
import org.infinispan.config.ConfigurationElement;
import org.infinispan.config.ConfigurationElements;
import org.infinispan.config.ConfigurationProperties;
import org.infinispan.config.ConfigurationProperty;
import org.infinispan.config.parsing.TreeNode;
import org.infinispan.tools.doclet.html.HtmlGenerator;
import org.infinispan.util.ClassFinder;

/**
 * Infinispan configuration reference guide generator
 *
 * @author Vladimir Blagojevic
 * @version $Id$
 * @since 4.0
 */
public class ConfigHtmlGenerator extends HtmlGenerator {

   String classpath;

   public ConfigHtmlGenerator(String encoding, String title, String bottom, String footer,
                              String header, String metaDescription, List<String> metaKeywords, String classpath) {
      super(encoding, title, bottom, footer, header, metaDescription, metaKeywords);
      this.classpath = classpath;

   }

   protected List<Class<?>> getConfigBeans() throws Exception {
      return ClassFinder.isAssignableFrom(ClassFinder.infinispanClasses(classpath), AbstractConfigurationBean.class);
   }

   protected String generateContents() {
      
      StringBuilder sb = new StringBuilder();
      // index of components
      sb.append("<h2>Infinispan configuration options</h2><br/>");
      sb.append("<UL>");

      List<Class<?>> configBeans;
      try {
         configBeans = getConfigBeans();
         XMLTreeOutputWalker tw = new XMLTreeOutputWalker(sb);
         TreeNode root = tw.constructTreeFromBeans(configBeans);                 
         
         sb.append("<div class=\"" +"source" + "\"><pre>");
         //print xml tree into StringBuilder
         tw.preOrderTraverse(root);
         sb.append("</pre></div>");
         for (Class<?> clazz : configBeans) {            
            ConfigurationElement ces[] = configurationElementsOnBean(clazz);            
            for (ConfigurationElement ce : ces) {
               boolean createdAttributes = false;
               boolean createdProperties = false;
               //Name, description, parent and child elements for ce ConfigurationElement
               generateHeaderForConfigurationElement(sb, tw, root, ce); 
               for (Method m : clazz.getMethods()) {
                  ConfigurationAttribute a = m.getAnnotation(ConfigurationAttribute.class);
                  boolean attribute = a != null && a.containingElement().equals(ce.name());
                  if (attribute && !createdAttributes) {
                     // Attributes
                     sb.append("<table class=\"bodyTable\"> ");
                     sb.append("<tr class=\"a\"><th>Attribute</th><th>Type</th><th>Default</th><th>Description</th></tr>\n");
                     createdAttributes = true;
                  }
                  if (attribute) {
                     generateAttributeTableRow(sb, m, a);
                  }
               }
               if (createdAttributes) {
                  sb.append("</table></div>");
               }
               
               for (Method m : clazz.getMethods()) {
                  ConfigurationProperty[] cprops = propertiesElementsOnMethod(m);
                  for (ConfigurationProperty c : cprops) {
                     boolean property = c.parentElement().equals(ce.name());
                     if (property && !createdProperties) {
                        // custom properties
                        sb.append("\n<table class=\"bodyTable\"> ");
                        sb.append("<tr class=\"a\"><th>Property</th><th>Description</th></tr>\n");
                        createdProperties = true;
                     }
                     if (property) {
                        generatePropertyTableRow(sb, c);
                     }
                  }
               }
               if (createdProperties) {
                  sb.append("</table></div>");
               }
            }
         }         
      } catch (Exception e) {
      }
      return sb.toString();
   }

   private void generatePropertyTableRow(StringBuilder sb, ConfigurationProperty c) {
      sb.append("<tr class=\"b\">");
      sb.append("<td>").append(c.name()).append("</td>\n");                              
      if(c.description().length() >0)
         sb.append("<td>").append(c.description()).append("</td>\n");
      else 
         sb.append("<td>").append("todo").append("</td>\n"); 
      sb.append("</tr>\n");
   }

   private ConfigurationProperty[] propertiesElementsOnMethod(Method m) {
      ConfigurationProperty[] cprops = new ConfigurationProperty[0];
      ConfigurationProperties cp = m.getAnnotation(ConfigurationProperties.class);
      ConfigurationProperty p = null;
      if (cp != null) {
         cprops = cp.elements();
      } else {
         p = m.getAnnotation(ConfigurationProperty.class);
         if (p != null) {
            cprops = new ConfigurationProperty[]{p};
         }
      }
      return cprops;
   }

   private void generateAttributeTableRow(StringBuilder sb, Method m, ConfigurationAttribute a) {
      sb.append("<tr class=\"b\">");
      sb.append("<td>").append("<code>" + a.name() +"</code>").append("</td>\n");
      
      sb.append("<td>").append("<code>" + m.getParameterTypes()[0].getSimpleName() + "</code>");
      if(a.allowedValues().length()>0){
         sb.append("*  " + a.allowedValues() +"</td>\n");
      } else{
         sb.append("</td>\n");
      }    
     
      //if default value specified in annotation use it
      if (a.defaultValue().length() > 0) {
         sb.append("<td>").append(a.defaultValue()).append("</td>\n");
      }

      //otherwise reflect that field and read default value
      else {
         try {
            //reflect default value 
            Object matchingFieldValue = matchingFieldValue(m);
            sb.append("<td>").append(matchingFieldValue).append("</td>\n");
         } catch (Exception e) {
            sb.append("<td>").append("N/A").append("</td>\n");
         }
      }                   
      if(a.description().length() >0)
         sb.append("<td>").append(a.description()).append("</td>\n");
      else 
         sb.append("<td>").append("todo").append("</td>\n");
      
      sb.append("</tr>\n");
   }

   private void generateHeaderForConfigurationElement(StringBuilder sb, XMLTreeOutputWalker tw,
            TreeNode root, ConfigurationElement ce) {
      sb.append("\n<a name=\"").append("ce_" + ce.parent() +"_" +ce.name() +"\">" + "</a>");
      sb.append("<div class=\"section\"><h3><a name=\"" + ce.name() + "\"></a>" + ce.name() +"</h3>");
      sb.append("\n<p>");
      if (ce.description().length() > 0) {
         sb.append(ce.description());
      } else {
         sb.append("todo");
      }
      
      TreeNode n = tw.findNode(root,ce.name(),ce.parent());
      sb.append(" Parent element is " + "<a href=\"").append("#ce_" + n.getParent().getParent().getName() + 
               "_" + n.getParent().getName()+ "\">" + "&lt;" + ce.parent() + "&gt;" + "</a>.");    
      
      if(!n.getChildren().isEmpty()){
         sb.append(" Child elements are ");
         int childCount = n.getChildren().size();
         int count = 1;
         for (TreeNode tn : n.getChildren()) {
            sb.append("<a href=\"").append("#ce_" + tn.getParent().getName() + "_" 
                     + tn.getName() + "\">" + "&lt;" + tn.getName() + "&gt;" + "</a>");
            if (count < childCount) {
               sb.append(",");
            } else {
               sb.append(".");
            }
            count++;
         }
         sb.append("\n");
      }
      sb.append("</p>");
   }

   private ConfigurationElement[] configurationElementsOnBean(Class<?> clazz) {
      ConfigurationElements configurationElements = clazz.getAnnotation(ConfigurationElements.class);
      ConfigurationElement configurationElement = clazz.getAnnotation(ConfigurationElement.class);
      ConfigurationElement ces [] = new ConfigurationElement[0];
      if (configurationElement != null && configurationElements == null) {
         ces = new ConfigurationElement[]{configurationElement};
      }
      if (configurationElements != null && configurationElement == null) {
         ces = configurationElements.elements();
      }
      return ces;
   }
   
   private boolean isSetterMethod(Method m) {
      return m.getName().startsWith("set") && m.getParameterTypes().length == 1;
   }

   private Object matchingFieldValue(Method m) throws Exception {
      String name = m.getName();
      if (!name.startsWith("set")) throw new IllegalArgumentException("Not a setter method");

      String fieldName = name.substring(name.indexOf("set") + 3);
      fieldName = fieldName.substring(0, 1).toLowerCase() + fieldName.substring(1);
      Field f = m.getDeclaringClass().getDeclaredField(fieldName);
      return getField(f, m.getDeclaringClass().newInstance());

   }

   private static Object getField(Field field, Object target) {
      if (!Modifier.isPublic(field.getModifiers())) {
         field.setAccessible(true);
      }
      try {
         return field.get(target);
      }
      catch (IllegalAccessException iae) {
         throw new IllegalArgumentException("Could not get field " + field, iae);
      }
   }
}
