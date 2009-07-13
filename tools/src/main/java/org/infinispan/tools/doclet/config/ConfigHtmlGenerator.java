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
import org.infinispan.tools.doclet.html.HtmlGenerator;
import org.infinispan.tools.schema.TreeNode;
import org.infinispan.tools.schema.XMLTreeOutputWalker;
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
            ConfigurationElement ces[] = null;
            ConfigurationElements configurationElements = clazz.getAnnotation(ConfigurationElements.class);
            ConfigurationElement configurationElement = clazz.getAnnotation(ConfigurationElement.class);

            if (configurationElement != null && configurationElements == null) {
               ces = new ConfigurationElement[]{configurationElement};
            }
            if (configurationElements != null && configurationElement == null) {
               ces = configurationElements.elements();
            }
            if (ces != null) {
               for (ConfigurationElement ce : ces) {
                  boolean createdAttributes = false;
                  boolean createdProperties = false;
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
                  
                  if(n != null && !n.getChildren().isEmpty()){
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
                  for (Method m : clazz.getMethods()) {
                     ConfigurationAttribute a = m.getAnnotation(ConfigurationAttribute.class);
                     boolean childElement = a != null && a.containingElement().equals(ce.name());
                     if (childElement && !createdAttributes) {
                        // Attributes
                        sb.append("<table class=\"bodyTable\"> ");
                        sb.append("<tr class=\"a\"><th>Attribute</th><th>Type</th><th>Default</th><th>Description</th></tr>\n");
                        createdAttributes = true;
                     }
                     if (childElement) {
                        sb.append("<tr class=\"b\">");
                        sb.append("<td>").append("<code>" + a.name() +"</code>").append("</td>\n");
                        
                        //if allowed values specified for attribute, use it
                        if (a.allowedValues().length() > 0) {
                           sb.append("<td>").append("<code>" + a.allowedValues()+"</code>").append("</td>\n");
                        }
                        //otherwise, reflect method and use parameter as allowed value
                        else if (isSetterMethod(m)) {
                           sb.append("<td>").append("<code>" + m.getParameterTypes()[0].getSimpleName() + "</code>").append("</td>\n");
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
                  }
                  if (createdAttributes) {
                     sb.append("</table></div>");
                  }
                  
                  for (Method m : clazz.getMethods()) {
                     ConfigurationProperty[] cprops = null;
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

                     if (cprops != null) {
                        for (ConfigurationProperty c : cprops) {
                           boolean child = c.parentElement().equals(ce.name());
                           if (child && !createdProperties) {
                              //custom properties         
                              sb.append("\n<table class=\"bodyTable\"> ");
                              sb.append("<tr class=\"a\"><th>Property</th><th>Description</th></tr>\n");        
                              createdProperties = true;
                           }
                           if (child) {
                              sb.append("<tr class=\"b\">");
                              sb.append("<td>").append(c.name()).append("</td>\n");                              
                              if(c.description().length() >0)
                                 sb.append("<td>").append(c.description()).append("</td>\n");
                              else 
                                 sb.append("<td>").append("todo").append("</td>\n"); 
                              sb.append("</tr>\n");
                           }
                        }
                     }
                  }
                  if (createdProperties) {
                     sb.append("</table></div>");
                  }                
               }
            }
         }
      } catch (Exception e) {
      }
      return sb.toString();
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
