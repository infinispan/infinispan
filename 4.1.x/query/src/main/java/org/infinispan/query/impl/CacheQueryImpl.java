/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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

package org.infinispan.query.impl;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.hibernate.search.FullTextFilter;
import org.hibernate.search.SearchException;
import org.hibernate.search.engine.DocumentBuilder;
import org.hibernate.search.engine.DocumentBuilderIndexedEntity;
import org.hibernate.search.engine.DocumentExtractor;
import org.hibernate.search.engine.FilterDef;
import org.hibernate.search.engine.SearchFactoryImplementor;
import org.hibernate.search.filter.ChainedFilter;
import org.hibernate.search.filter.FilterKey;
import org.hibernate.search.filter.StandardFilterKey;
import org.hibernate.search.query.FullTextFilterImpl;
import org.hibernate.search.query.QueryHits;
import org.hibernate.search.reader.ReaderProvider;
import org.hibernate.search.store.DirectoryProvider;
import org.hibernate.transform.ResultTransformer;
import org.infinispan.Cache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.QueryIterator;
import org.infinispan.query.backend.IndexSearcherCloser;
import org.infinispan.query.backend.KeyTransformationHandler;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hibernate.search.reader.ReaderProviderHelper.getIndexReaders;
import static org.hibernate.search.util.FilterCacheModeTypeHelper.cacheInstance;
import static org.hibernate.search.util.FilterCacheModeTypeHelper.cacheResults;

/**
 * Implementation class of the CacheQuery interface.
 * <p/>
 *
 * @author Navin Surtani
 */
public class CacheQueryImpl implements CacheQuery {
   private Sort sort;
   private Filter filter;
   private Map<String, FullTextFilterImpl> filterDefinitions;
   private Integer firstResult;
   private Integer resultSize;
   private Integer maxResults;
   private boolean needClassFilterClause;
   private String[] indexProjection;
   private ResultTransformer resultTransformer;
   private Set<Class<?>> classesAndSubclasses;
   private Set<String> idFieldNames;
   private boolean allowFieldSelectionInProjection = true;
   private Query luceneQuery;
   private SearchFactoryImplementor searchFactory;
   private Set<Class<?>> targetedEntities;
   private Cache cache;

   public org.infinispan.util.logging.Log log;


   public CacheQueryImpl(Query luceneQuery, SearchFactoryImplementor searchFactory, Cache cache, Class... classes) {
      this.luceneQuery = luceneQuery;
      this.cache = cache;
      this.searchFactory = searchFactory;
      if (classes == null || classes.length == 0) {
         this.targetedEntities = this.searchFactory.getIndexedTypesPolymorphic(new Class[]{});
      } else {
         this.targetedEntities = this.searchFactory.getIndexedTypesPolymorphic(classes);
      }
   }


   /**
    * Takes in a lucene filter and sets it to the filter field in the class.
    *
    * @param f - lucene filter
    */

   public void setFilter(Filter f) {
      filter = f;
   }


   /**
    * @return The result size of the query.
    */
   public int getResultSize() {
      if (resultSize == null) {
         //get result size without object initialization
         IndexSearcher searcher = buildSearcher(searchFactory);
         if (searcher == null) {
            resultSize = 0;
         } else {
            TopDocs hits;
            try {
               hits = getQueryHits(searcher, 1).topDocs; // Lucene enforces that at least one top doc will be retrieved.
               resultSize = hits.totalHits;
            }
            catch (IOException e) {
               throw new SearchException("Unable to query Lucene index", e);
            }
            finally {
               //searcher cannot be null
               try {
                  closeSearcher(searcher, searchFactory.getReaderProvider());
                  //searchFactoryImplementor.getReaderProvider().closeReader( searcher.getIndexReader() );
               }
               catch (SearchException e) {
                  log.warn("Unable to properly close searcher during lucene query: " + e);
               }
            }
         }
      }
      return this.resultSize;
   }

   private void closeSearcher(Searcher searcher, ReaderProvider readerProvider) {
      Set<IndexReader> indexReaders = getIndexReaders(searcher);

      for (IndexReader indexReader : indexReaders) {
         readerProvider.closeReader(indexReader);
      }
   }


   public void setSort(Sort s) {
      sort = s;
   }


   /**
    * Enable a given filter by its name.
    *
    * @param name of filter.
    * @return a FullTextFilter object.
    */
   public FullTextFilter enableFullTextFilter(String name) {
      if (filterDefinitions == null) {
         filterDefinitions = new HashMap<String, FullTextFilterImpl>();
      }
      FullTextFilterImpl filterDefinition = filterDefinitions.get(name);
      if (filterDefinition != null) return filterDefinition;

      filterDefinition = new FullTextFilterImpl();
      filterDefinition.setName(name);
      FilterDef filterDef = searchFactory.getFilterDefinition(name);
      if (filterDef == null) {
         throw new SearchException("Unkown @FullTextFilter: " + name);
      }
      filterDefinitions.put(name, filterDefinition);
      return filterDefinition;
   }

   /**
    * Disable a given filter by its name.
    *
    * @param name of filter.
    */
   public void disableFullTextFilter(String name) {
      filterDefinitions.remove(name);
   }

   /**
    * Sets the the result of the given integer value to the first result.
    *
    * @param firstResult index to be set.
    * @throws IllegalArgumentException if the index given is less than zero.
    */
   public void setFirstResult(int firstResult) {
      if (firstResult < 0) {
         throw new IllegalArgumentException("'first' pagination parameter less than 0");
      }
      this.firstResult = firstResult;

   }

   public QueryIterator iterator() throws SearchException {
      return iterator(1);
   }

   public QueryIterator iterator(int fetchSize) throws SearchException {
      List<Object> keyList = null;
      IndexSearcher searcher = buildSearcher(searchFactory);
      if (searcher == null) {
         throw new NullPointerException("IndexSearcher instance is null.");
      }

      try {
         QueryHits queryHits = getQueryHits(searcher, calculateTopDocsRetrievalSize());
         int first = first();
         int max = max(first, queryHits.totalHits);
         int size = max - first + 1 < 0 ? 0 : max - first + 1;
         keyList = new ArrayList<Object>(size);

         DocumentExtractor extractor = new DocumentExtractor(queryHits, searchFactory, indexProjection, idFieldNames, allowFieldSelectionInProjection);
         for (int index = first; index <= max; index++) {
            // Since the documentId is same thing as the key in each key, value pairing. We can just get the documentId
            // from Lucene and then get it from the cache.

            // The extractor.extract.id gives me the documentId that we need.


            String keyString = (String) extractor.extract(index).id;
            keyList.add(KeyTransformationHandler.stringToKey(keyString));
         }

      }
      catch (IOException e) {
         throw new SearchException("Unable to query Lucene index", e);

      }

      finally {

         IndexSearcherCloser.closeSearcher(searcher, searchFactory.getReaderProvider());

      }

      return new EagerIterator(keyList, cache, fetchSize);
   }

   public QueryIterator lazyIterator() {
      return lazyIterator(1);
   }

   public QueryIterator lazyIterator(int fetchSize) {
      IndexSearcher searcher = buildSearcher(searchFactory);

      try {
         QueryHits queryHits = getQueryHits(searcher, calculateTopDocsRetrievalSize());
         int first = first();
         int max = max(first, queryHits.totalHits);

         DocumentExtractor extractor = new DocumentExtractor(queryHits, searchFactory, indexProjection, idFieldNames, allowFieldSelectionInProjection);

         return new LazyIterator(extractor, cache, searcher, searchFactory, first, max, fetchSize);
      }
      catch (IOException e) {
         try {
            IndexSearcherCloser.closeSearcher(searcher, searchFactory.getReaderProvider());
         }
         catch (SearchException ee) {
            //we have the initial issue already
         }
         throw new SearchException("Unable to query Lucene index", e);

      }

   }

   public List<Object> list() throws SearchException {
      IndexSearcher searcher = buildSearcher(searchFactory);

      if (searcher == null) return Collections.EMPTY_LIST;


      try {

         QueryHits queryHits = getQueryHits(searcher, calculateTopDocsRetrievalSize());

         int first = first();
         int max = max(first, queryHits.totalHits);

         int size = max - first + 1 < 0 ? 0 : max - first + 1;

         DocumentExtractor extractor = new DocumentExtractor(queryHits, searchFactory, indexProjection, idFieldNames, allowFieldSelectionInProjection);

         List<String> keysForCache = new ArrayList<String>(size);
         for (int index = first; index <= max; index++) {
            // Since the documentId is same thing as the key in each key, value pairing. We can just get the documentId
            // from Lucene and then get it from the cache.

            // The extractor.extract.id gives me the documentId that we need.

            String cacheKey = extractor.extract(index).id.toString(); // these are always strings
            keysForCache.add(cacheKey);
         }

         // Loop through my list of keys and get it from the cache. Put each object that I get into a separate list.
         List<Object> listToReturn = new ArrayList<Object>(size);
         for (String key : keysForCache) listToReturn.add(cache.get(KeyTransformationHandler.stringToKey(key)));

         // TODO: navssurtani --> Speak with EB or HF about what a resultTransformer is and what it does etc etc.

         if (resultTransformer == null) {
            return listToReturn;
         } else {
            return resultTransformer.transformList(listToReturn);

         }

      }
      catch (IOException e) {
         throw new SearchException("Unable to query Lucene index", e);

      }
      finally {
         IndexSearcherCloser.closeSearcher(searcher, searchFactory.getReaderProvider());

      }

   }

   private int max(int first, int totalHits) {
      if (maxResults == null) {
         return totalHits - 1;
      } else {
         return maxResults + first < totalHits ?
               first + maxResults - 1 :
               totalHits - 1;
      }
   }

   private int first() {
      return firstResult != null ?
            firstResult :
            0;
   }

   private QueryHits getQueryHits(Searcher searcher, Integer n) throws IOException {
      org.apache.lucene.search.Query query = filterQueryByClasses(luceneQuery);
      buildFilters();
      QueryHits queryHits;
      if (n == null) { // try to make sure that we get the right amount of top docs
         queryHits = new QueryHits(searcher, query, filter, sort);
      } else {
         queryHits = new QueryHits(searcher, query, filter, sort, n);
      }
      resultSize = queryHits.totalHits;
      return queryHits;
   }

   private Integer calculateTopDocsRetrievalSize() {
      if (maxResults == null) {
         return null;
      } else {
         return first() + maxResults;
      }
   }


   public void setMaxResults(int maxResults) {
      if (maxResults < 0) {
         throw new IllegalArgumentException("'max' pagination parameter less than 0");
      }
      this.maxResults = maxResults;
   }

   private IndexSearcher buildSearcher(SearchFactoryImplementor searchFactoryImplementor) {
      Map<Class<?>, DocumentBuilderIndexedEntity<?>> builders = searchFactoryImplementor.getDocumentBuildersIndexedEntities();
      List<DirectoryProvider> directories = new ArrayList<DirectoryProvider>();
      Set<String> idFieldNames = new HashSet<String>();
      Similarity searcherSimilarity = null;
      if (targetedEntities.isEmpty()) {
         // empty targetedEntities array means search over all indexed enities,
         // but we have to make sure there is at least one
         if (builders.isEmpty()) {
            throw new SearchException (
                  "There are no mapped entities. Don't forget to add @Indexed to at least one class."
            );
         }

         for (DocumentBuilderIndexedEntity builder : builders.values()) {
            searcherSimilarity = checkSimilarity(searcherSimilarity, builder);
            if (builder.getIdKeywordName() != null) {
               idFieldNames.add(builder.getIdKeywordName());
               allowFieldSelectionInProjection = allowFieldSelectionInProjection && builder.allowFieldSelectionInProjection();
            }
            final DirectoryProvider[] directoryProviders = builder.getDirectoryProviderSelectionStrategy()
                  .getDirectoryProvidersForAllShards();
            populateDirectories(directories, directoryProviders);
         }
         classesAndSubclasses = null;
      } else {
         Set<Class<?>> involvedClasses = new HashSet<Class<?>>(targetedEntities.size());
         involvedClasses.addAll(targetedEntities);
         for (Class<?> clazz : targetedEntities) {
            DocumentBuilderIndexedEntity<?> builder = builders.get(clazz);
            if (builder != null) {
               involvedClasses.addAll(builder.getMappedSubclasses());
            }
         }

         for (Class clazz : involvedClasses) {
            DocumentBuilderIndexedEntity builder = builders.get(clazz);
            if (builder == null) {
               throw new SearchException ("Not a mapped entity (don't forget to add @Indexed): " + clazz);
            }
            if (builder.getIdKeywordName() != null) {
               idFieldNames.add(builder.getIdKeywordName());
               allowFieldSelectionInProjection = allowFieldSelectionInProjection && builder.allowFieldSelectionInProjection();
            }
            final DirectoryProvider[] directoryProviders = builder.getDirectoryProviderSelectionStrategy()
                  .getDirectoryProvidersForAllShards();
            searcherSimilarity = checkSimilarity(searcherSimilarity, builder);
            populateDirectories(directories, directoryProviders);
         }
         this.classesAndSubclasses = involvedClasses;
      }
      this.idFieldNames = idFieldNames;

      //compute optimization needClassFilterClause
      //if at least one DP contains one class that is not part of the targeted classesAndSubclasses we can't optimize
      if (classesAndSubclasses != null) {
         for (DirectoryProvider dp : directories) {
            final Set<Class<?>> classesInDirectoryProvider = searchFactoryImplementor.getClassesInDirectoryProvider(
                  dp
            );
            // if a DP contains only one class, we know for sure it's part of classesAndSubclasses
            if (classesInDirectoryProvider.size() > 1) {
               //risk of needClassFilterClause
               for (Class clazz : classesInDirectoryProvider) {
                  if (!classesAndSubclasses.contains(clazz)) {
                     this.needClassFilterClause = true;
                     break;
                  }
               }
            }
            if (this.needClassFilterClause) {
               break;
            }
         }
      }

      //set up the searcher
      final DirectoryProvider[] directoryProviders = directories.toArray(new DirectoryProvider[directories.size()]);
      IndexSearcher is = new IndexSearcher(
            searchFactoryImplementor.getReaderProvider().openReader(
                  directoryProviders
            )
      );
      is.setSimilarity(searcherSimilarity);
      return is;
   }


   private Similarity checkSimilarity(Similarity similarity, DocumentBuilderIndexedEntity builder) {
      if (similarity == null) {
         similarity = builder.getSimilarity();
      } else if (!similarity.getClass().equals(builder.getSimilarity().getClass())) {
         throw new SearchException("Cannot perform search on two entities with differing Similarity implementations (" + similarity.getClass().getName() + " & " + builder.getSimilarity().getClass().getName() + ")");
      }

      return similarity;
   }

   private void populateDirectories(List<DirectoryProvider> directories, DirectoryProvider[] directoryProviders)

   {
      for (DirectoryProvider provider : directoryProviders) {
         if (!directories.contains(provider)) {
            directories.add(provider);
         }
      }
   }


   private org.apache.lucene.search.Query filterQueryByClasses(org.apache.lucene.search.Query luceneQuery) {
      if (!needClassFilterClause) {
         return luceneQuery;
      } else {
         //A query filter is more practical than a manual class filtering post query (esp on scrollable resultsets)
         //it also probably minimise the memory footprint
         BooleanQuery classFilter = new BooleanQuery();
         //annihilate the scoring impact of DocumentBuilder.CLASS_FIELDNAME
         classFilter.setBoost(0);
         for (Class clazz : classesAndSubclasses) {
            Term t = new Term(DocumentBuilder.CLASS_FIELDNAME, clazz.getName());
            TermQuery termQuery = new TermQuery(t);
            classFilter.add(termQuery, BooleanClause.Occur.SHOULD);
         }
         BooleanQuery filteredQuery = new BooleanQuery();
         filteredQuery.add(luceneQuery, BooleanClause.Occur.MUST);
         filteredQuery.add(classFilter, BooleanClause.Occur.MUST);
         return filteredQuery;
      }
   }


   // Method changed by Navin Surtani on Dec 16th 2008. Copied out from FullTextQueryImpl from Hibernate Search code like
   // previously done. Also copied in methods like buildLuceneFilter(), createFilter() and those methods that follow down
   // until the end of the class.
   private void buildFilters() {
      if (filterDefinitions == null || filterDefinitions.isEmpty()) {
         return; // there is nothing to do if we don't have any filter definitions
      }

      ChainedFilter chainedFilter = new ChainedFilter();
      for (FullTextFilterImpl fullTextFilter : filterDefinitions.values()) {
         Filter filter = buildLuceneFilter(fullTextFilter);
         chainedFilter.addFilter(filter);
      }

      if (filter != null) {
         chainedFilter.addFilter(filter);
      }
      filter = chainedFilter;
   }

   private Filter buildLuceneFilter(FullTextFilterImpl fullTextFilter) {

      /*
      * FilterKey implementations and Filter(Factory) do not have to be threadsafe wrt their parameter injection
      * as FilterCachingStrategy ensure a memory barrier between concurrent thread calls
      */
      FilterDef def = searchFactory.getFilterDefinition(fullTextFilter.getName());
      Object instance = createFilterInstance(fullTextFilter, def);
      FilterKey key = createFilterKey(def, instance);

      // try to get the filter out of the cache
      Filter filter = cacheInstance(def.getCacheMode()) ?
            searchFactory.getFilterCachingStrategy().getCachedFilter(key) :
            null;

      if (filter == null) {
         filter = createFilter(def, instance);

         // add filter to cache if we have to
         if (cacheInstance(def.getCacheMode())) {
            searchFactory.getFilterCachingStrategy().addCachedFilter(key, filter);
         }
      }
      return filter;
   }

   private Filter createFilter(FilterDef def, Object instance) {
      Filter filter;
      if (def.getFactoryMethod() != null) {
         try {
            filter = (Filter) def.getFactoryMethod().invoke(instance);
         }
         catch (IllegalAccessException e) {
            throw new SearchException(
                  "Unable to access @Factory method: "
                        + def.getImpl().getName() + "." + def.getFactoryMethod().getName()
            );
         }
         catch (InvocationTargetException e) {
            throw new SearchException(
                  "Unable to access @Factory method: "
                        + def.getImpl().getName() + "." + def.getFactoryMethod().getName()
            );
         }
         catch (ClassCastException e) {
            throw new SearchException(
                  "@Key method does not return a org.apache.lucene.search.Filter class: "
                        + def.getImpl().getName() + "." + def.getFactoryMethod().getName()
            );
         }
      } else {
         try {
            filter = (Filter) instance;
         }
         catch (ClassCastException e) {
            throw new SearchException(
                  "Filter implementation does not implement the Filter interface: "
                        + def.getImpl().getName() + ". "
                        + (def.getFactoryMethod() != null ? def.getFactoryMethod().getName() : ""), e
            );
         }
      }

      filter = addCachingWrapperFilter(filter, def);
      return filter;
   }

   private Object createFilterInstance(FullTextFilterImpl fullTextFilter,
                                       FilterDef def) {
      Object instance;
      try {
         instance = def.getImpl().newInstance();
      }
      catch (InstantiationException e) {
         throw new SearchException("Unable to create @FullTextFilterDef: " + def.getImpl(), e);
      }
      catch (IllegalAccessException e) {
         throw new SearchException("Unable to create @FullTextFilterDef: " + def.getImpl(), e);
      }
      for (Map.Entry<String, Object> entry : fullTextFilter.getParameters().entrySet()) {
         def.invoke(entry.getKey(), instance, entry.getValue());
      }
      if (cacheInstance(def.getCacheMode()) && def.getKeyMethod() == null && !fullTextFilter.getParameters().isEmpty()) {
         throw new SearchException("Filter with parameters and no @Key method: " + fullTextFilter.getName());
      }
      return instance;
   }


   private FilterKey createFilterKey(FilterDef def, Object instance) {
      FilterKey key = null;
      if (!cacheInstance(def.getCacheMode())) {
         return key; // if the filter is not cached there is no key!
      }

      if (def.getKeyMethod() == null) {
         key = new FilterKey() {
            public int hashCode() {
               return getImpl().hashCode();
            }

            public boolean equals(Object obj) {
               if (!(obj instanceof FilterKey)) {
                  return false;
               }
               FilterKey that = (FilterKey) obj;
               return this.getImpl().equals(that.getImpl());
            }
         };
      } else {
         try {
            key = (FilterKey) def.getKeyMethod().invoke(instance);
         }
         catch (IllegalAccessException e) {
            throw new SearchException(
                  "Unable to access @Key method: "
                        + def.getImpl().getName() + "." + def.getKeyMethod().getName()
            );
         }
         catch (InvocationTargetException e) {
            throw new SearchException(
                  "Unable to access @Key method: "
                        + def.getImpl().getName() + "." + def.getKeyMethod().getName()
            );
         }
         catch (ClassCastException e) {
            throw new SearchException(
                  "@Key method does not return FilterKey: "
                        + def.getImpl().getName() + "." + def.getKeyMethod().getName()
            );
         }
      }
      key.setImpl(def.getImpl());

      //Make sure Filters are isolated by filter def name
      StandardFilterKey wrapperKey = new StandardFilterKey();
      wrapperKey.addParameter(def.getName());
      wrapperKey.addParameter(key);
      return wrapperKey;
   }

   private Filter addCachingWrapperFilter(Filter filter, FilterDef def) {
      if (cacheResults(def.getCacheMode())) {
         int cachingWrapperFilterSize = searchFactory.getFilterCacheBitResultsSize();
         filter = new org.hibernate.search.filter.CachingWrapperFilter(filter, cachingWrapperFilterSize);
      }

      return filter;
   }

}

