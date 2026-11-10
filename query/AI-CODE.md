# Query Module Instructions

- Built on top of Hibernate Search.
- `TermsAggregationOptionsStep.maxTermCount(int)` controls the maximum number of buckets in a terms aggregation (default 100).
- Method chaining order matters: `.maxTermCount()` must be called after `.value()`, not before. `.field()` returns `TermsAggregationValueStep` (which has `.value()`), and `.value()` returns `TermsAggregationOptionsStep` (which has `.maxTermCount()`).

## UPDATE BY QUERY / expression architecture

Parse flow: query string → `IckleLexer`/`IckleParser` (ANTLR) → `IckleQueryStringParser.parse` →
`IckleParserVisitorResult` (builds the AST; validates property paths, function names and value types) →
`IckleParsingResult` → `QueryEngine.buildQuery` → one of `EmbeddedQuery`, `HybridQuery`, `IndexedQueryImpl`,
`AggregatingQuery`, `ExpressionProjectionQuery` (SELECT expressions) or the clustered `CQUpdate`.
All UPDATE execution funnels through `UpdateQueryHelper` — the single choke point for how updates are applied.

| Concern | Location (under `query/src/main/java`) |
|---|---|
| Grammar, incl. the value-expression ladder (`updateExpression/Term/Factor/Primary`) | `src/main/antlr4/org/infinispan/query/grammar/` |
| Parse → AST, shape checks | `objectfilter/impl/syntax/parser/IckleParserVisitorResult.java`, `IckleQueryStringParser.java` |
| Parse result | `objectfilter/impl/syntax/parser/IckleParsingResult.java` (`UpdateOperation`, `hasExpressionProjections`) |
| Value-expression AST, evaluator, type checker, function registry | `objectfilter/impl/syntax/update/` |
| Built-in functions / UDF SPI | `org/infinispan/query/functions/` (`QueryFunction`, `QueryFunctionContext`, `builtin/*`) |
| SELECT expression evaluation (in-memory, over an inner query) | `core/impl/ExpressionProjectionQuery.java` |
| Update execution, all strategies (`UpdateBiFunction`, `ProtobufFieldAccessor`, `ReflectionResolver`) | `core/impl/UpdateQueryHelper.java` |
| WHERE predicate AST | `objectfilter/impl/syntax/*.java` — deliberately separate from the value-expression tree |
| Tests | `UpdateGrammarTest` (grammar/lexer), `UpdateValueEvaluatorTest`/`UpdateValueTypeCheckerTest` (unit), `UpdateQueryCoreTest` (e2e), `NestedUpdatePathTest`/`ReflectionNestedUpdatePathTest` (apply paths), `DistributedUpdateByQueryTest` (clustered) |

Non-obvious invariants:

- The parse result never crosses the wire: clustered execution (`CQUpdate`) re-parses the query string on the
  owner node, so the value AST is plain Java with no protostream annotations. `UpdateBiFunction`, which *is*
  marshalled, carries the query string and named parameters — never the parsed AST.
- The value-expression AST is a separate hierarchy from the WHERE predicate tree; expression work must not
  add visit methods to the predicate `Visitor` pipeline.
- An expression supports at most one division operator per line: the lexer's longest-match turns a second `/`
  into a `REGEXP_LITERAL`. Use the `divide(a, b)` function for repeated division. Pinned by `UpdateGrammarTest`.
- Updates are atomic per statement (snapshot-and-rollback in `UpdateQueryHelper`) but not against concurrent
  writers — tracked in #18623.
- Statements whose values are all plain constants take the legacy fast path and never enter expression
  evaluation; keep that property when changing the execution paths.
