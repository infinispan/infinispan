/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.infinispan.config.ConfigurationDoc;
import org.infinispan.config.ConfigurationDocRef;
import org.infinispan.config.ConfigurationDocs;
import org.infinispan.tools.doclet.html.HtmlGenerator;
import org.infinispan.tools.schema.TreeNode;
import org.infinispan.tools.schema.XSOMSchemaTreeWalker;
import org.infinispan.util.ReflectionUtil;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.Doc;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.MethodDoc;
import com.sun.javadoc.Parameter;
import com.sun.javadoc.RootDoc;
import com.sun.xml.xsom.XSAttributeDecl;
import com.sun.xml.xsom.XSFacet;
import com.sun.xml.xsom.XSRestrictionSimpleType;
import com.sun.xml.xsom.XSSchemaSet;
import com.sun.xml.xsom.parser.XSOMParser;

@SuppressWarnings("restriction")
public abstract class AbstractConfigHtmlGenerator extends HtmlGenerator {

   protected static final String CONFIG_REF = "configRef";

   protected static final String CONFIG_REF_NAME_ATT = "name";
   protected static final String CONFIG_REF_PARENT_NAME_ATT = "parentName";
   protected static final String CONFIG_REF_DESC_ATT = "desc";

   private static final int LEVEL_MULT = 3;
   private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("infinispan.tools.configdoc.debug", "false"));

   protected RootDoc rootDoc;
   protected StringBuilder sb;

   public AbstractConfigHtmlGenerator(String encoding, String title, String bottom, String footer,
            String header, String metaDescription, List<String> metaKeywords) {
      super(encoding, title, bottom, footer, header, metaDescription, metaKeywords);
      sb = new StringBuilder();
   }

   protected abstract List<Class<?>> getConfigBeans() throws Exception;

   /**
    * Returns name of the schema file.
    * 
    * <p>
    * Note that schema file should be placed on a classpath.
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
    * @return true if the TreeNode n should be skipped for configuration reference creation, false
    *         otherwise
    */
   protected boolean preVisitNode(TreeNode n) {
      return true;
   }

   /**
    * Callback invoked after visiting the specified node n.
    * 
    * @param n
    * @return true if no more elements should be included in configuration reference creation, false
    *         otherwise
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
      InputStream is = filename == null || filename.length() == 0 ? null
               : getAsInputStreamFromClassLoader(filename);
      if (is == null) {
         try {
            is = new FileInputStream(filename);
         } catch (FileNotFoundException e) {
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

   @Override
   protected String generateContents() {
      sb.append(getTitle());

      List<Class<?>> configBeans;
      try {
         configBeans = getConfigBeans();

         if (configBeans == null || configBeans.isEmpty())
            throw new Exception(
                     "Configuration bean classes are not specified. Make sure that "
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
         try {
            reader.parse(file);
         } finally {
            file.close();
         }
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
            // do not generate element for skipped element
            if (skip)
               continue;

            sb.append("<div class=\"section\">\n");

            debug("Generating " + n + " bean is " + n.getBeanClass());

            // Name, description, parent and child elements for node
            generateHeaderForConfigurationElement(sb, tw, n);

            // now attributes
            if (!n.getAttributes().isEmpty()) {
               generateAttributeTableRows(sb, n);
            }

            boolean breakLoop = postVisitNode(n);
            sb.append("</div>\n");
            if (breakLoop)
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
               if (clazz.isAnnotationPresent(ConfigurationDoc.class)) {
                  associate(clazz.getAnnotation(ConfigurationDoc.class), n, clazz);
               } else if (clazz.isAnnotationPresent(ConfigurationDocs.class)) {
                  ConfigurationDoc[] docs = clazz.getAnnotation(ConfigurationDocs.class).value();
                  for (ConfigurationDoc cd : docs) {
                     associate(cd, n, clazz);
                  }
               }
            }
         }
      }
   }

   private void associate(ConfigurationDoc cd, TreeNode n, Class<?> clazz) {
      if (cd != null) {
         String thisNode = cd.name();
         String parentNode = cd.parentName();
         if (n.getName().equalsIgnoreCase(thisNode)) {
            if (parentNode.equalsIgnoreCase(n.getParent().getName())) {
               debug("Parent associated " + clazz + " with node " + n.getName() + ", parent " + parentNode);
               n.setBeanClass(clazz);
            } else if (parentNode.length() == 0) {
               debug("Normal associated " + clazz + " with node " + n.getName());
               n.setBeanClass(clazz);
            }
         }
      }
   }

   private void generateAttributeTableRows(StringBuilder sb, TreeNode n) {

      sb.append("<table class=\"bodyTable\"> ");
      sb.append("<tr class=\"a\"><th>Attribute</th><th>Type</th><th>Default</th><th>Description</th></tr>\n");

      Class<?> bean = n.getBeanClass();
      Object beanClassInstance = null;
      try {
         Constructor<?>[] constructors = bean.getDeclaredConstructors();
         for (Constructor<?> c : constructors) {
            if (c.getParameterTypes().length == 0) {
               c.setAccessible(true);
               beanClassInstance = c.newInstance();
            }
         }
      } catch (Exception e) {
         throw new RuntimeException("TreeNode " + n.getName() + " is associated with a bean class "
                  + bean + " whose instantiation failed on default contructor ");
      }
      
      if(beanClassInstance == null)
         throw new RuntimeException("Bean class could not be instantied, aborting!");
         
      Set<XSAttributeDecl> attributes = n.getAttributes();
      for (XSAttributeDecl a : attributes) {
         generateTableRowForAttribute(a, sb, beanClassInstance);
      }
      sb.append("</table>\n");
   }

   protected void generateTableRowForAttribute(XSAttributeDecl a, StringBuilder sb, Object beanClassInstance) {
      sb.append("<tr class=\"b\">");

      // name, type...
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

      debug("attribute = " + a.getName() + "(restricted = " + isRestricted + ")", 1);

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

      Field field = findField(beanClassInstance.getClass(), a.getName());
      if (field == null) {
         throw new RuntimeException("Null field for " + beanClassInstance.getClass() + " attribute "
                  + a.getName());
      }

      // if default value specified in annotation use it
      if (a.getDefaultValue() != null) {
         debug("annotation-defined default = " + a.getDefaultValue(), 2);
         sb.append("<td>").append(a.getDefaultValue().toString()).append("</td>\n");
      } else {
         // otherwise use reflected field and read default value
         Object defaultValue = null;
         try {
            defaultValue = ReflectionUtil.getValue(beanClassInstance, field.getName());
            if (defaultValue != null) {
               sb.append("<td>").append(defaultValue.toString()).append("</td>\n");
               debug("field-defined default = " + defaultValue, 2);
            } else {
               debug("field-defined default is null!", 2);
               sb.append("<td>").append("null").append("</td>\n");
            }
         } catch (Exception e) {
            debug("Caught exception, bean is " + beanClassInstance.getClass() + ", looking for field "
                     + a.getName() + ", field " + field, 2);
            e.printStackTrace();
            sb.append("<td>").append("N/A").append("</td>\n");
         }
      }

      // and finally description
      String desc = null;
      Doc docElement = null;
      ConfigurationDocRef docRef = null;
      if (field.isAnnotationPresent(ConfigurationDoc.class)) {
         desc = field.getAnnotation(ConfigurationDoc.class).desc();
      } else if (field.isAnnotationPresent(ConfigurationDocRef.class)) {
         docRef = field.getAnnotation(ConfigurationDocRef.class);
         docElement = findDocElement(docRef.bean(), docRef.targetElement());
         desc = docElement.commentText();
      }
      if (desc != null) {
         sb.append("<td>").append(desc).append("\n");            
         String htmlFile = field.getDeclaringClass().getName().replace(".", "/").replace("$",".").concat(".html");
         //overridden by ConfigurationDocRef?
         if(docRef != null) {
            htmlFile = docRef.bean().getName().replace(".", "/").replace("$",".").concat(".html");
            if(docElement instanceof MethodDoc){
               MethodDoc mDocElement = (MethodDoc)docElement;
               Parameter[] parameters = mDocElement.parameters();
               
               //if this is MethodDoc then docRef is pointing to a method, get targetElement
               String targetElement = docRef.targetElement();
               StringBuilder javadocTarget = new StringBuilder(targetElement);
               javadocTarget.append("(");
               for (Parameter parameter : parameters) {
                  javadocTarget.append(parameter.type().qualifiedTypeName()).append(","); 
               }
               javadocTarget.deleteCharAt(javadocTarget.length()-1);
               javadocTarget.append(")");
               sb.append(" (<a href=\"" + htmlFile.concat("#").concat(javadocTarget.toString()) + "\">"+ "Javadoc</a>)");
            }
         } else {
            sb.append(" (<a href=\"" + htmlFile.concat("#").concat(field.getName()) + "\">"+ "Javadoc</a>)");
         }            
         sb.append("</td>\n");
      }
      sb.append("</tr>\n");
   }

   private void debug(String s, int level) {
      if (DEBUG) {
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < level * LEVEL_MULT; i++)
            sb.append(" ");
         sb.append("> ").append(s);
         System.out.println(sb.toString());
      }
   }

   private void debug(String s) {
      debug(s, 0);
   }

   private boolean validString(String s) {
      return s != null && s.length() > 0;
   }

   protected void generateHeaderForConfigurationElement(StringBuilder sb, XMLTreeOutputWalker tw,
            TreeNode n) {
      sb.append("<a name=\"").append("ce_" + n.getParent().getName() + "_" + n.getName() + "\">" + "</a>");
      sb.append("<h3><a name=\"" + n.getName() + "\"></a>" + n.getName() + "</h3>");
      sb.append("\n<p>");
      Class<?> beanClass = n.getBeanClass();
      Map<String, String> desc = findDescription(beanClass);
      if (!desc.isEmpty()) {
         for (Entry<String, String> e : desc.entrySet()) {
            if (n.getName().equals(e.getKey())) {
               sb.append(e.getValue());
            }
         }
      }

      sb.append("<BR/><BR />");

      if (n.getParent().getParent() != null) {
         sb.append("The parent element is " + "<a href=\"").append("#ce_" 
                  + n.getParent().getParent().getName() + "_" + n.getParent().getName()
                           + "\">" + "&lt;" + n.getParent().getName() + "&gt;" + "</a>.  ");
      }

      if (!n.getChildren().isEmpty()) {
         int childCount = n.getChildren().size();
         int count = 1;
         if (childCount == 1)
            sb.append("The only child element is ");
         else
            sb.append("Child elements are ");
         for (TreeNode tn : n.getChildren()) {
            sb.append("<a href=\"").append("#ce_" + tn.getParent().getName() + 
                     "_" + tn.getName() + "\">" + "&lt;" + tn.getName() + "&gt;" + "</a>");
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
   
   protected Field findField(Class<?> clazz, String name) {
      Field f = null;
      boolean found = false;
      Class<?> current = clazz;
      while (current != null) {
         try {
            f = current.getDeclaredField(name);
            return f;
         } catch (NoSuchFieldException e) {
            current = current.getSuperclass();
         }
      }

      List<Field> anFields = ReflectionUtil.getAnnotatedFields(clazz, ConfigurationDoc.class);
      for (Field field : anFields) {
         if (field.getAnnotation(ConfigurationDoc.class).name().equals(name)) {
            f = field;
            found = true;
            break;
         }
      }

      // or with ConfigurationDocReference....
      if (!found) {
         anFields = ReflectionUtil.getAnnotatedFields(clazz, ConfigurationDocRef.class);
         for (Field field : anFields) {
            if (field.getAnnotation(ConfigurationDocRef.class).name().equals(name)) {
               f = field;
               break;
            }
         }
      }
      return f;
   }

   protected Map<String, String> findDescription(AnnotatedElement e) {
      Map<String, String> m = new HashMap<String, String>();
      ConfigurationDoc cd = e.getAnnotation(ConfigurationDoc.class);
      if (cd != null) {
         extractConfigurationDocComments(e, m, cd);
      } else if (e.isAnnotationPresent(ConfigurationDocs.class)) {
         ConfigurationDoc[] configurationDocs = e.getAnnotation(ConfigurationDocs.class).value();
         for (ConfigurationDoc cd2 : configurationDocs) {
            extractConfigurationDocComments(e, m, cd2);
         }
      }
      return m;
   }

   protected void extractConfigurationDocComments(AnnotatedElement e, Map<String, String> m,
            ConfigurationDoc cd) {
      if (cd != null) {
         if (validString(cd.desc())) {
            m.put(cd.name(), cd.desc());
         } else {
            if (e instanceof Class<?>) {
               Class<?> clazz = (Class<?>) e;
               ClassDoc classDoc = rootDoc.classNamed(clazz.getName());
               m.put(cd.name(), classDoc.commentText());
            }
         }
      }
   }

   protected Doc findDocElement(Class<?> c, String elementName) {
      while (true) {
         ClassDoc classDoc = rootDoc.classNamed(c.getName());
         for (MethodDoc md : classDoc.methods()) {
            if (md.name().equalsIgnoreCase(elementName)) {
               return md;
            }
         }
         for (FieldDoc fd : classDoc.fields()) {
            if (fd.name().equalsIgnoreCase(elementName)) {
               return fd;
            }
         }
         if (c.getSuperclass() != null) {
            c = c.getSuperclass();
            continue;
         }
         return null;
      }
   }
}
