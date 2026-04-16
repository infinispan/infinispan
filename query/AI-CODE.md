# Query Module Instructions

- Built on top of Hibernate Search.
- `TermsAggregationOptionsStep.maxTermCount(int)` controls the maximum number of buckets in a terms aggregation (default 100).
- Method chaining order matters: `.maxTermCount()` must be called after `.value()`, not before. `.field()` returns `TermsAggregationValueStep` (which has `.value()`), and `.value()` returns `TermsAggregationOptionsStep` (which has `.maxTermCount()`).
