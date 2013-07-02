package org.infinispan.distexec.mapreduce;

import java.io.Serializable;

/**
 * Example object to run a query using MapReduce.
 * Using Java here, but it could be anything such
 * as free text, JSON, YAML, ...
 *
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
public class Book implements Serializable {

   /** The serialVersionUID */
   private static final long serialVersionUID = 201181615056852234L;
   
   final String title;
   final String author;
   final String editor;

   public Book(String title, String author, String editor) {
      this.title = title;
      this.author = author;
      this.editor = editor;
   }

}
