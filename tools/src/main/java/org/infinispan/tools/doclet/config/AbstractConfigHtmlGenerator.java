/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.tools.doclet.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.infinispan.tools.doclet.html.HtmlGenerator;
import org.infinispan.tools.schema.TreeNode;
import org.infinispan.tools.schema.XSOMSchemaTreeWalker;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.RootDoc;
import com.sun.javadoc.Tag;
import com.sun.xml.xsom.XSAttributeDecl;
import com.sun.xml.xsom.XSFacet;
import com.sun.xml.xsom.XSRestrictionSimpleType;
import com.sun.xml.xsom.XSSchemaSet;
import com.sun.xml.xsom.parser.XSOMParser;

@SuppressWarnings("restriction")
public abstract class AbstractConfigHtmlGenerator extends HtmlGenerator {
    
    protected static final String CONFIG_REF = "configRef";                                                                              
    
    protected static final String CONFIG_REF_NAME_ATT= "name";
    protected static final String CONFIG_REF_PARENT_NAME_ATT= "parentName";
    protected static final String CONFIG_REF_DESC_ATT= "desc";
    

    private static final int LEVEL_MULT = 3;
    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("infinispan.tools.configdoc.debug", "false"));
    
    protected RootDoc rootDoc;
    protected StringBuilder sb; 

    public AbstractConfigHtmlGenerator(String encoding, String title, String bottom, String footer,
                    String header, String metaDescription, List<String> metaKeywords) {
        super(encoding, title, bottom, footer, header, metaDescription, metaKeywords);
        sb = new StringBuilder();
    }

   /**
    * Returns a list of classes inspected for configuration reference javadoc tags. Configuration
    * reference tags are specified at class level and field level using <code>@configRef</code>
    * javadoc tag.
    * 
    * <p>
    * Configuration class hierarchy should match configuration XML schema where each
    * configuration class matches to one Java class and each property of XML element matches to a
    * field of a class. On a class level <code>@configRef</code> javadoc tag should be placed on a
    * class definition that matches the target XML element. On a field level <code>@configRef</code>
    * javadoc tag should be placed on a field (anywhere in class hiearchy) that matches XML
    * property.
    * 
    * <p>
    * <code>@configRef</code> has two key value property pairs: name and desc. Name specifies the
    * name of the matching XML element in cases when <code>@configRef</code> decorates a class. If
    * <code>@configRef</code> decorates a Java field then name attribute should match XML attribute.
    * 
    * <p>
    * Note that name property is optional in cases when <code>@configRef</code> decorates a Java
    * field while it is mandatory in cases when <code>@configRef</code> decorates a configuration
    * class.
    * 
    * 
    * 
    * @return a list of configuration classes decorated with <code>@configRef</code> Javadoc tags.
    * @throws Exception
    */
   protected abstract List<Class<?>> getConfigBeans() throws Exception;
    
    /**
    * Returns name of the schema file. 
    * 
    * <p>Note that schema file should be placed on a classpath. 
    * 
    * @return name of the schema file located on the classpath. 
    */
   protected abstract String getSchemaFile();
    
    /**
    * Name of the root element in the schema
    * 
    * @return name of the root element in the schema
    */
   protected abstract String getRootElementName();
    
   /**
    * Invoked prior to creation of XML tree table of contents for configuration elements in schema
    * 
    * @param sw
    * @param tw
    */
   protected void preXMLTableOfContentsCreate(XSOMSchemaTreeWalker sw, XMLTreeOutputWalker tw) {

   }

   /**
    * Invoked after creation of XML tree table of contents for configuration elements in schema
    * 
    * @param sw
    * @param tw
    */
   protected void postXMLTableOfContentsCreate(XSOMSchemaTreeWalker w, XMLTreeOutputWalker tw) {

   }
    
    /**
    * Callback invoked prior to visiting the specified node n.
    * 
    * @param n
    * @return true if the TreeNode n should be skipped for configuration reference creation, false otherwise
    */
   protected boolean preVisitNode(TreeNode n) {
        return true;
    }
    
   /**
    * Callback invoked after visiting the specified node n.
    * 
    * @param n
    * @return true if no more elements should be included in configuration reference creation, false otherwise
    */
    protected boolean postVisitNode(TreeNode n) {
        return false;
    }
    
    protected String getTitle() {
        return "<h2>Configuration reference</h2><br/>";
    }

    public RootDoc getRootDoc() {
       return rootDoc;
    }

    public void setRootDoc(RootDoc rootDoc) {
       this.rootDoc = rootDoc;
    }
    
    public InputStream lookupFile(String filename) {
        InputStream is = filename == null || filename.length() == 0 ? null : getAsInputStreamFromClassLoader(filename);
        if (is == null) {       
           try {
              is = new FileInputStream(filename);
           }
           catch (FileNotFoundException e) {
              return null;
           }
        }
        return is;
     }
    
    protected InputStream getAsInputStreamFromClassLoader(String filename) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl == null ? null : cl.getResourceAsStream(filename);
        if (is == null) {
           // check system class loader
           is = getClass().getClassLoader().getResourceAsStream(filename);
        }
        return is;
     }
    
    protected StringBuilder getStringBuilder() {
        return sb;
    }

    protected String generateContents() {      
       sb.append(getTitle());

       List<Class<?>> configBeans;
       try {
          configBeans = getConfigBeans();
          
          if (configBeans == null || configBeans.isEmpty())
             throw new Exception("Configuration bean classes are not specified. Make sure that "
                              + "getConfigBeans() method returns a list of classes. Documentation creation aborted");

          XMLTreeOutputWalker tw = new XMLTreeOutputWalker(sb);
          String schemaFile = getSchemaFile();
          
          if (schemaFile == null)
             throw new Exception("Schema file name not specified. Documentation creation aborted");

          InputStream file = lookupFile(schemaFile);
          
         if (file == null)
            throw new Exception("Schema file " + schemaFile
                     + " not found on classpath. Documentation creation aborted");
         
          XSOMParser reader = new XSOMParser();
          reader.parse(file);
          XSSchemaSet xss = reader.getResult();
          XSOMSchemaTreeWalker w = new XSOMSchemaTreeWalker(xss.getSchema(1), getRootElementName());
          TreeNode root = w.getRoot();
          associateBeansWithTreeNodes(configBeans, root);
          
          preXMLTableOfContentsCreate(w, tw);

          sb.append("<div class=\"" + "source" + "\"><pre>");
          // print XMLTableOfContents into StringBuilder
          tw.preOrderTraverse(root);
          sb.append("</pre></div>");
          
          postXMLTableOfContentsCreate(w, tw);


          for (TreeNode n : root) {

              boolean skip = preVisitNode(n);
             //do not generate element for skipped element 
             if (skip)
                continue;

             debug("element = " + n.getName(), 0);

             sb.append("<div class=\"section\">\n");
             // Name, description, parent and child elements for node
             generateHeaderForConfigurationElement(sb, tw, n);

             //now attributes
             if (!n.getAttributes().isEmpty()) {
                generateAttributeTableRows(sb, n);
             }
             
             boolean  breakLoop = postVisitNode(n);
             sb.append("</div>\n");
             if(breakLoop) 
                 break;                          
          }

       } catch (Exception e) {
          System.out.println("Exception while generating configuration reference " + e); 
          e.printStackTrace();
       }
       return sb.toString();
    }

    private void associateBeansWithTreeNodes(List<Class<?>> configBeans, TreeNode root) {
       for (TreeNode n : root) {
          if (n.getBeanClass() == null) {
             for (Class<?> clazz : configBeans) {
                ClassDoc classDoc = rootDoc.classNamed(clazz.getName());
                if (classDoc != null) {
                   List<Tag> list = Arrays.asList(classDoc.tags(CONFIG_REF));
                   for (Tag tag : list) {
                      String text = tag.text().trim();
                      Map<String, String> p = parseTag(text);
                      String thisNode = p.get(CONFIG_REF_NAME_ATT);
                      String parentNode = p.get(CONFIG_REF_PARENT_NAME_ATT);
                      if (n.getName().equalsIgnoreCase(thisNode)) {
                         // parent specified
                         if (parentNode != null
                               && parentNode.equalsIgnoreCase(n.getParent().getName())) {
                            n.setBeanClass(clazz);
                         } else if (parentNode == null) {
                            n.setBeanClass(clazz);
                         }
                      }
                   }
                }
             }
          }
       }
    }

    private void generateAttributeTableRows(StringBuilder sb, TreeNode n) {

       sb.append("<table class=\"bodyTable\"> ");
       sb.append("<tr class=\"a\"><th>Attribute</th><th>Type</th><th>Default</th><th>Description</th></tr>\n");

       Class<?> bean = n.getBeanClass();
       Object object = null;
       try {
          Constructor<?>[] constructors = bean.getDeclaredConstructors();
          for (Constructor<?> c : constructors) {
             if (c.getParameterTypes().length == 0) {
                c.setAccessible(true);
                object = c.newInstance();
             }
          }
       } catch (Exception e) {
          System.out.println("Did not construct object " + bean);
       }

       Set<XSAttributeDecl> attributes = n.getAttributes();
       for (XSAttributeDecl a : attributes) {
          sb.append("<tr class=\"b\">");

          //name, type...
          sb.append("<td>").append("<code>" + a.getName() + "</code>").append("</td>\n");
          sb.append("<td>").append("<code>" + a.getType().getName() + "</code>");

          boolean isRestricted = false;
          XSRestrictionSimpleType restriction = a.getType().asRestriction();
          Collection<? extends XSFacet> declaredFacets = restriction.getDeclaredFacets();
          for (XSFacet facet : declaredFacets) {
             if (facet.getName().equalsIgnoreCase("enumeration")) {
                isRestricted = true;
                break;
             }
          }

          debug ("attribute = " + a.getName() + "(restricted = " + isRestricted + ")", 1);

          // restriction on type...
          if (isRestricted) {
             sb.append("* (");
             for (XSFacet facet : declaredFacets) {
                sb.append(facet.getValue().toString() + '|');
             }
             sb.deleteCharAt(sb.length() - 1);
             sb.append(")</td>\n");
          } else {
             sb.append("</td>\n");
          }

          // if default value specified in annotation use it
          if (a.getDefaultValue() != null) {
             debug("annotation-defined default = " + a.getDefaultValue(), 2);
             sb.append("<td>").append(a.getDefaultValue().toString()).append("</td>\n");
          }

          // otherwise reflect that field and read default value
          else {
             Field field = null;
             Object defaultValue = null;
             try {
                field = findFieldRecursively(bean, a.getName());
                defaultValue = fieldValue(field, object);
                if (defaultValue != null) {
                   sb.append("<td>").append(defaultValue.toString()).append("</td>\n");
                   debug("field-defined default = " + defaultValue, 2);
                } else {
                   debug("field-defined default is null!", 2);
                   sb.append("<td>").append("null").append("</td>\n");
                }
             } catch (Exception e) {
                debug("Caught exception, bean is " + bean.getName() + ", looking for field " + a.getName() + ", field " + field, 2);               
                e.printStackTrace();
                sb.append("<td>").append("N/A").append("</td>\n");
             }
          }

          // and finally description
          FieldDoc fieldDoc = findFieldDocRecursively(bean, a.getName(), CONFIG_REF);
          if (fieldDoc != null) {
             Tag[] tags = fieldDoc.tags(CONFIG_REF);
             Map<String, String> p = parseTag(tags[0].text().trim());
             sb.append("<td>").append(p.get(CONFIG_REF_DESC_ATT)).append("\n");
             String packageDir = fieldDoc.containingPackage().toString().replace(".", "/").concat("/");
             String htmlFile = fieldDoc.containingClass().typeName().concat(".html");
             String field = fieldDoc.name();
             sb.append(" (<a href=\"" +packageDir.concat(htmlFile).concat("#").concat(field) +"\">" + "Javadoc</a>)");
             sb.append("</td>\n");
             
          }
          sb.append("</tr>\n");
       }
       sb.append("</table>\n");
    }

    private void debug(String s, int level) {
       if (DEBUG) {
          StringBuilder sb = new StringBuilder();
          for (int i = 0; i < level * LEVEL_MULT; i++) sb.append(" ");
          sb.append("> ").append(s);
          System.out.println(sb.toString());
       }
    }

    public Map<String, String> parseTag(String tag) {

       //javadoc parser for our tags
       Map<String, String> p = new HashMap<String, String>();
       Scanner sc = new Scanner(tag);
       Scanner sc2 = null;
       sc.useDelimiter("\"\\s*,\\s*");
       try {
          while (sc.hasNext()) {
             String keyValue = sc.next();
             sc2 = new Scanner(keyValue);
             sc2.useDelimiter("=\\s*\"");
             String key = sc2.next();
             String value = sc2.next().replace("\"", "");
             p.put(key, value);
             sc2.close();
          }
          sc.close();
       } catch (Exception e) {
          System.out.println("Invalid tag " + tag + " skipping...");
       } finally {
          sc.close();
          sc2.close();
       }
       return p;
    }

    private void generateHeaderForConfigurationElement(StringBuilder sb, XMLTreeOutputWalker tw, TreeNode n) {
       sb.append("<a name=\"").append("ce_" + n.getParent().getName() + "_" + n.getName() + "\">" + "</a>");
       sb.append("<h3><a name=\"" + n.getName() + "\"></a>" + n.getName() + "</h3>");
       sb.append("\n<p>");
       Class<?> beanClass = n.getBeanClass();
       //System.out.println("Generating " + n + " bean is " + beanClass);
       ClassDoc classDoc = rootDoc.classNamed(beanClass.getName());
       Tag[] tags = classDoc.tags(CONFIG_REF);
       
       for (Tag tag : tags) {
          String text = tag.text().trim();
          Map<String, String> m = parseTag(text);
          String name = m.get(CONFIG_REF_NAME_ATT);
          if(n.getName().equals(name)) {
              sb.append(m.get(CONFIG_REF_DESC_ATT));
          }
       }

       sb.append("<BR/><BR />");

       if (n.getParent().getParent() != null) {
          sb.append("The parent element is " + "<a href=\"").append("#ce_" + n.getParent().getParent().getName()
                + "_" + n.getParent().getName() + "\">" + "&lt;" + n.getParent().getName() + "&gt;" + "</a>.  ");
       }

       if (!n.getChildren().isEmpty()) {
          int childCount = n.getChildren().size();
          int count = 1;
          if (childCount == 1)
             sb.append("The only child element is ");
          else
             sb.append("Child elements are ");
          for (TreeNode tn : n.getChildren()) {
             sb.append("<a href=\"").append("#ce_" + tn.getParent().getName() + "_"
                   + tn.getName() + "\">" + "&lt;" + tn.getName() + "&gt;" + "</a>");
             if (count < childCount) {
                sb.append(", ");
             } else {
                sb.append(".");
             }
             count++;
          }
          sb.append("\n");
       }
       sb.append("</p>");
    }

    private Field findFieldRecursively(Class<?> c, String fieldName) {
       findFieldRecursively:
       while (true) {
          Field f = null;
          try {
             f = c.getDeclaredField(fieldName);
          } catch (NoSuchFieldException e) {
             ClassDoc classDoc = rootDoc.classNamed(c.getName());
             for (FieldDoc fd : classDoc.fields()) {
                for (Tag t : fd.tags(CONFIG_REF)) {
                   Map<String, String> m = parseTag(t.text().trim());
                   String field = m.get(CONFIG_REF_NAME_ATT);
                   if (field != null && field.startsWith(fieldName)) {
                      fieldName = fd.name();
                      continue findFieldRecursively;
                   }
                }
             }
             if (!c.equals(Object.class))
                f = findFieldRecursively(c.getSuperclass(), fieldName);
          }
          return f;
       }
    }

    private FieldDoc findFieldDocRecursively(Class<?> c, String fieldName, String tagName) {  
       while (true) {
          ClassDoc classDoc = rootDoc.classNamed(c.getName());
          for (FieldDoc fd : classDoc.fields()) {
             if (fd.name().equalsIgnoreCase(fieldName)) {
                return fd;
             }
             for (Tag t : fd.tags(tagName)) {
                Map<String, String> m = parseTag(t.text().trim());
                if (m.containsKey(CONFIG_REF_NAME_ATT)) {
                   String value = m.get(CONFIG_REF_NAME_ATT).trim();
                   if (fieldName.equalsIgnoreCase(value)) {
                      return fd;
                   }
                }
             }
          }
          if (c.getSuperclass() != null) {
             c = c.getSuperclass();
             continue;
          }
          return null;
       }
    }

    private static Object fieldValue(Field field, Object target) {
       if (!Modifier.isPublic(field.getModifiers())) {
          field.setAccessible(true);
       }
       try {
          return field.get(target);
       } catch (IllegalAccessException iae) {
          throw new IllegalArgumentException("Could not get field " + field, iae);
       }
    }
}
