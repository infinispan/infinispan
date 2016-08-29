/*
 * Copyright 2016, Red Hat Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.infinispan.objectfilter.impl.ql;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.ql.parse.IckleLexer;
import org.infinispan.objectfilter.impl.ql.parse.IckleParser;
import org.infinispan.objectfilter.impl.ql.parse.QueryRenderer;
import org.infinispan.objectfilter.impl.ql.parse.QueryResolver;
import org.jboss.logging.Logger;

/**
 * A parser for Ickle queries. Parsing comprises these steps:
 * <ul>
 * <li>lexing the query</li>
 * <li>parsing the query, building up an AST while doing so</li>
 * <li>transforming the resulting parse tree using a QueryResolverDelegate and QueryRendererDelegate</li>
 * </ul>
 *
 * @author Gunnar Morling
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class QueryParser {

   private static final Log log = Logger.getMessageLogger(Log.class, QueryParser.class.getName());

   /**
    * Parses the given query string.
    *
    * @param queryString the query string to parse
    * @return the result of the parsing after being transformed by the processors
    * @throws ParsingException in case any exception occurs during parsing
    */
   public CommonTree parseQuery(String queryString, QueryResolverDelegate resolverDelegate, QueryRendererDelegate rendererDelegate) throws ParsingException {
      IckleLexer lexer = new IckleLexer(new ANTLRStringStream(queryString));
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      IckleParser parser = new IckleParser(tokens);

      try {
         // parser.statement() is the entry point for evaluation of any kind of statement
         IckleParser.statement_return r = parser.statement();

         if (parser.hasErrors()) {
            throw log.getQuerySyntaxException(queryString, parser.getErrorMessages());
         }

         CommonTree tree = (CommonTree) r.getTree();
         tree = resolve(tokens, tree, resolverDelegate);
         tree = render(tokens, tree, rendererDelegate);
         return tree;
      } catch (RecognitionException e) {
         throw log.getQuerySyntaxException(queryString, e);
      }
   }

   // resolves the elements in given source query into an output query, by invoking {@link QueryResolverDelegate} while traversing the given query tree
   private CommonTree resolve(TokenStream tokens, CommonTree tree, QueryResolverDelegate resolverDelegate) throws RecognitionException {
      CommonTreeNodeStream treeNodeStream = new CommonTreeNodeStream(tree);
      treeNodeStream.setTokenStream(tokens);
      return (CommonTree) new QueryResolver(treeNodeStream, resolverDelegate).statement().getTree();
   }

   // render a given source query into an output query, by invoking {@link QueryRendererDelegate} while traversing the given query tree
   private CommonTree render(TokenStream tokens, CommonTree tree, QueryRendererDelegate rendererDelegate) throws RecognitionException {
      CommonTreeNodeStream treeNodeStream = new CommonTreeNodeStream(tree);
      treeNodeStream.setTokenStream(tokens);
      return (CommonTree) new QueryRenderer(treeNodeStream, rendererDelegate).statement().getTree();
   }
}
