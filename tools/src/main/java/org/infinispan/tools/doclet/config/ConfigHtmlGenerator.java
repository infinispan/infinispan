package org.infinispan.tools.doclet.config;

import org.infinispan.config.AbstractConfigurationBean;
import org.infinispan.config.ConfigurationAttribute;
import org.infinispan.config.ConfigurationElement;
import org.infinispan.config.ConfigurationElements;
import org.infinispan.config.ConfigurationProperties;
import org.infinispan.config.ConfigurationProperty;
import org.infinispan.tools.doclet.html.HtmlGenerator;
import org.infinispan.util.ClassFinder;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
         TreeNode root  = tree(configBeans);     
         
         sb.append("<div class=\"" +"source" + "\"><pre>");
         sb.append(root.pp(""));
         sb.append("</pre></div>");
         
         tree(configBeans);
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
                  TreeNode n = findNode(root,ce.name(),ce.parent());
                  sb.append(" Parent element is " + "<a href=\"").append(
                           "#ce_" + n.parent.parent.name + "_" + n.parent.name + "\">" + "&lt;"
                                    + ce.parent() + "&gt;" + "</a>.");    
                  
                  if(n != null && !n.children.isEmpty()){
                     sb.append(" Child elements are ");
                     int childCount = n.children.size();
                     int count = 1;
                     for (TreeNode tn : n.children) {
                        sb.append("<a href=\"").append(
                                 "#ce_" + tn.parent.name + "_" + tn.name + "\">" + "&lt;"
                                          + tn.name + "&gt;" + "</a>");
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
   
   private TreeNode findNode(TreeNode tn, String name, String parent){
      TreeNode result = null;
      if(tn.name.equals(name) && tn.parent != null && tn.parent.name.equals(parent)){         
         result = tn;
      } else {
         for (TreeNode child :tn.children){
            result = findNode(child,name,parent);
            if(result != null) break;
         }
      }
      return result;
   }
   
   private TreeNode tree(List<Class<?>>configBeans){
      List<ConfigurationElement> lce = new ArrayList<ConfigurationElement>(7);
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
         if(ces != null){
            lce.addAll(Arrays.asList(ces));
         }
      }
      TreeNode root = new TreeNode();
      root.parent = new TreeNode();
      root.name = "infinispan";
      makeTree(lce,root);
      return root;
   }
   
   private void makeTree(List<ConfigurationElement> lce, TreeNode tn) {
      for (ConfigurationElement ce : lce) {
         if(ce.parent().equals(tn.name)){
            TreeNode child = new TreeNode();
            child.name = ce.name();
            child.parent = tn;         
            tn.children.add(child);
            makeTree(lce,child);
         }
      }
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
   
   private class TreeNode {
      String name = "";
      TreeNode parent;
      Set<TreeNode> children = new HashSet<TreeNode>();

      public String toString() {
         return name;
      }

      public String pp(String prefix) {
         StringBuffer result = new StringBuffer(prefix + "&lt;<a href=\"" + "#ce_" + parent.name
                  + "_" + name + "\">" + name + "</a>&gt;" + "\n");
         String newPrefix = prefix + "  ";
         for (TreeNode child : children)
            result.append(child.pp(newPrefix));
         return result.toString();
      }

      public boolean equals(Object other) {
         if (other == this)
            return true;
         if (!(other instanceof TreeNode))
            return false;
         TreeNode tn = (TreeNode) other;
         return this.parent.name != null && tn.parent != null
                  && this.parent.name.equals(tn.parent.name) && this.name.equals(tn.name);
      }

      public int hashCode() {
         int result = 17;
         result = 31 * result + name.hashCode();
         result = 31 * result
                  + ((parent != null && parent.name != null) ? parent.name.hashCode() : 0);
         return result;
      }
   }
}
