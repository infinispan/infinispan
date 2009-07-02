package org.infinispan.tools.doclet.html;

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
import org.infinispan.tools.ClassFinder;

public class ConfigHtmlGenerator extends HtmlGenerator {


   public ConfigHtmlGenerator(String encoding, String title, String bottom, String footer,
            String header, String metaDescription, List<String> metaKeywords) {
      super(encoding, title, bottom, footer, header, metaDescription, metaKeywords);
     
   }

   protected String generateContents() {
      StringBuilder sb = new StringBuilder();
      // index of components
      sb.append("<h2>Infinispan configuration options</h2><br />");
      sb.append("<UL>");
      
      List<Class<?>> list;
      try {
         list = ClassFinder.isAssignableFrom(AbstractConfigurationBean.class);                  
         for(Class<?> clazz: list){
            ConfigurationElement ces [] = null;
            ConfigurationElements configurationElements = clazz.getAnnotation(ConfigurationElements.class);
            ConfigurationElement configurationElement = clazz.getAnnotation(ConfigurationElement.class);
            
            if(configurationElement != null && configurationElements == null){
               ces = new ConfigurationElement[]{configurationElement};
            }
            if(configurationElements != null && configurationElement ==null){
               ces = configurationElements.elements();
            }            
            if(ces != null){                              
               for(ConfigurationElement ce:ces){
                  
                  boolean createdAttributes = false;
                  boolean createdProperties = false;                  
                  sb.append("<A NAME=\"").append(ce.name()).append("\">\n");
                  sb.append("\n<TABLE WIDTH=\"100%\" CELLSPACING=\"1\" CELLPADDING=\"0\" BORDER=\"1\">\n");
                  sb.append("<TR CLASS=\"TableHeadingColor\"><TH ALIGN=\"LEFT\"><b>Element: <tt> ").append(ce.name()).append(", </tt></b>");
                  sb.append("Parent element: <tt>").append(ce.parent()).append("</tt>");
                  if(ce.description().length() > 0){
                     sb.append(" ").append(ce.description()).append("\n");
                  } else {
                     sb.append("\n");
                  }                 
                  for(Method m:clazz.getMethods()){
                     ConfigurationAttribute a = m.getAnnotation(ConfigurationAttribute.class);
                     boolean childElement = a != null && a.containingElement().equals(ce.name());
                     if(childElement && !createdAttributes){
                     // Attributes                        
                        sb.append("<TR CLASS=\"TableSubHeadingColor\"><TH ALIGN=\"LEFT\"><strong><i>Attributes</i></strong></TH></TR>\n");
                        sb.append("<TR BGCOLOR=\"white\" CLASS=\"TableRowColor\"><TD ALIGN=\"CENTER\"><TABLE WIDTH=\"100%\" cellspacing=\"1\" cellpadding=\"0\" border=\"0\">\n");
                        sb.append("<TR CLASS=\"TableSubHeadingColor\"><TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><strong>Name</strong></TD>\n");
                        sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\" WIDTH=\"40%\"><strong>Description</strong></TD>\n");
                        sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><strong>Default values</strong></TD>\n");
                        sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><strong>Allowed values</strong></TD>\n</TR>\n");
                        createdAttributes = true;
                     } 
                     if (childElement){    
                        sb.append("<TR BGCOLOR=\"white\" CLASS=\"TableRowColor\">");
                        sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><tt>").append(a.name()).append("</tt></TD>");
                        sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\">").append(a.description()).append("</TD>");
                        
                        //if default value specified in annotation use it
                        if(a.defaultValue().length() >0){
                           sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><tt>").append(a.defaultValue()).append("</tt></TD>");
                        }
                        
                        //otherwise reflect that field and read default value
                        else{
                           try{
                              //reflect default value 
                              Object matchingFieldValue = matchingFieldValue(m);
                              sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><tt>").append(matchingFieldValue).append("</tt></TD>");                             
                           } catch(Exception e){
                              sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><tt>").append("N/A").append("</tt></TD>");      
                           }
                        }
                        
                        //if allowed values specified for attribute, use it
                        if(a.allowedValues().length() > 0){
                           sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\">").append(a.allowedValues()).append("</TD>");
                        }
                        //otherwise, reflect method and use parameter as allowed value
                        else if(isSetterMethod(m)){                           
                           sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\">").append(m.getParameterTypes()[0].getSimpleName()).append("</TD>");
                        }
                        sb.append("\n</TR>");                              
                     }                     
                  }   
                  if (createdAttributes) {
                     sb.append("\n</TABLE></TD></TR>");
                  }
                
                     
                              
                  for(Method m:clazz.getMethods()){                     
                     ConfigurationProperty[] cprops = null;
                     ConfigurationProperties cp = m.getAnnotation(ConfigurationProperties.class);
                     ConfigurationProperty p = null;
                     if (cp != null) {
                        cprops = cp.elements();
                     } else {
                        p = m.getAnnotation(ConfigurationProperty.class);
                        if (p != null) {
                           cprops = new ConfigurationProperty[] { p };
                        }
                     }
                     
                     if(cprops != null){                        
                        for (ConfigurationProperty c : cprops) {
                           boolean child = c.parentElement().equals(ce.name());
                           if(child && !createdProperties){
                              //custom properties                                                            
                              sb.append("<TR CLASS=\"TableSubHeadingColor\"><TH ALIGN=\"LEFT\"><strong><i>Properties</i></strong></TH></TR>\n");
                              sb.append("<TR BGCOLOR=\"white\" CLASS=\"TableRowColor\"><TD ALIGN=\"CENTER\"><TABLE WIDTH=\"100%\" cellspacing=\"1\" cellpadding=\"0\" border=\"0\">\n");
                              sb.append("<TR CLASS=\"TableSubHeadingColor\"><TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><strong>Name</strong></TD>\n");
                              sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\" WIDTH=\"40%\"><strong>Description</strong></TD>\n</TR>\n");                             
                              createdProperties = true;
                           } 
                           if (child){    
                              sb.append("<TR BGCOLOR=\"white\" CLASS=\"TableRowColor\">");
                              sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><tt>").append(c.name()).append("</tt></TD>");
                              sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\">").append(c.description()).append("</TD>");                             
                              sb.append("\n</TR>");                                                  
                           }   
                        }                       
                     }                                          
                  }
                  if(createdProperties){
                     sb.append("\n</TABLE></TD></TR>");
                  }
                  
                  //closing table
                  sb.append("\n</TABLE></TD></TR>");
               }
            }
         }
      } catch (Exception e) {
      }      
      return sb.toString();
   }
   
   private boolean isSetterMethod(Method m){
      return m.getName().startsWith("set") && m.getParameterTypes().length ==1;
   }
   
   private Object matchingFieldValue(Method m) throws Exception{      
      String name = m.getName();
      if(!name.startsWith("set")) throw new IllegalArgumentException("Not a setter method");
      
      String fieldName = name.substring(name.indexOf("set") + 3);         
      fieldName = fieldName.substring(0, 1).toLowerCase() + fieldName.substring(1);         
      Field f = m.getDeclaringClass().getDeclaredField(fieldName);
      return getField(f, m.getDeclaringClass().newInstance());
     
   }
   
   private static Object getField(Field field, Object target) {
      if(!Modifier.isPublic(field.getModifiers())) {
          field.setAccessible(true);
      }
      try {
          return field.get(target);
      }
      catch(IllegalAccessException iae) {
          throw new IllegalArgumentException("Could not get field " + field, iae);
      }
  }
}
