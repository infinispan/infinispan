# Planned Library Functions — Roadmap for Future Phases

Status: catalog only — **none of these are part of the current work**.
They are candidates for future phases, modeled on the PostgreSQL
(`pg_catalog`) function catalog and filtered for usefulness in `UPDATE`
statement value expressions and `SELECT` projections.

Conventions for this document:
- *Pri* = suggested priority: **1** = high value / low cost, **2** = medium,
  **3** = niche or sizable.
- Names are proposals; existing functions keep their names.
- The PostgreSQL column shows the closest `pg_catalog` counterpart
  (`pgcrypto`/`contrib` noted where relevant).

---

## 1. Baseline — what already ships

| Category | Functions |
|---|---|
| Math | `abs`, `round`, `floor`, `ceil`, `min`, `max`, `power`, `sqrt` |
| String | `concat`, `upper`, `lower`, `trim`, `length`, `substring`, `replace` |
| Regexp | `regexp_like`, `regexp_replace`, `regexp_substr`, `regexp_instr` |
| Date | `adddays`, `addhours`, `addminutes`, `addmonths`, `addyears`, `dateadd` |
| Conversion | `tolong`, `todouble`, `tostring` |

## 2. Conventions every new function must follow

- Register in `QueryFunctionRegistry.createStandard()` (global singleton;
  `@NamedFactory` UDFs merge in at server start).
- Annotate `evaluate` with `@QueryFunctionSignature(args = ..., returns = ...,
  category = ...)`. The declared `returns` drives **parse-time type checking**
  of function results (ISPN028537) — declare the narrowest correct type.
- Case-insensitive names; names must not collide with builtins or registered
  UDFs (`add()` throws on duplicates).
- Null propagation: any `null` argument ⇒ `null` result (matches the
  arithmetic evaluator).
- Numeric promotion: integral in ⇒ integral out (`long`), any floating point
  in ⇒ `double` (matches the evaluator).
- No grammar change needed for plain function calls (the `identifier (...)`
  ladder already accepts any registered name).
- Ship with: unit tests (`UpdateValueEvaluatorTest`-style), one e2e test
  (`UpdateQueryCoreTest`-style), and a row in the function reference of
  `ref_query_ickle_syntax.adoc`.

Deliberate semantic deviations from PostgreSQL (do not copy blindly):
- **Rounding**: Java `Math.round` (half-up) semantics, not PostgreSQL's
  round-half-to-even.
- **Time zones**: no time-zone awareness; temporal values are wall-clock
  instants (PG functions like `timezone()` are a much larger feature).
- **String → temporal**: the parse-time checker rejects raw string-to-temporal
  assignment; the sanctioned path is an explicit conversion function
  (section 3.1).
- **Intervals**: PG returns an `interval` type where we have none; functions
  that measure time spans must pick a primitive representation (see notes).

---

## 3. Candidates by category

### 3.1 Date and time

| Proposed Ickle name | PostgreSQL counterpart | Purpose | Pri | Notes |
|---|---|---|---|---|
| `now()` | `now()`, `current_timestamp` | Current instant | 1 | Enables the ubiquitous `SET updatedAt = now()`; return the entity's temporal type |
| `extract(field, ts)` | `extract(field from ts)`, `date_part` | Extract year/month/day/hour/minute/second | 1 | Very high-frequency in reporting updates |
| `date_trunc(unit, ts)` | `date_trunc` | Truncate to a unit boundary (day, week, month, quarter, year) | 1 | Bucketing, period keys |
| `age(ts1, ts2)` | `age` | Span between two instants | 2 | PG returns `interval`; we must choose a primitive (days as `long` or millis) — decision needed |
| `to_timestamp(string)` | `to_timestamp(text)` | Parse ISO-8601 string to a temporal value | 2 | The sanctioned string→temporal conversion (parse-time checker rejects raw string assignment to temporal fields) |
| `to_date(string)` | `to_date` | Parse string to a date (midnight instant) | 2 | Same as above; format parameter optional |
| `date_bin(width, ts, origin)` | `date_bin` (PG 14) | Bin instants into fixed-width buckets | 3 | Analytics niche |
| `timezone(tz, ts)` | `timezone` | Shift an instant to a time zone | 3 | Requires a time-zone model — larger feature, not a one-off function |

### 3.2 String and text

| Proposed Ickle name | PostgreSQL counterpart | Purpose | Pri | Notes |
|---|---|---|---|---|
| `strpos(s, sub)` | `strpos`, `position(sub in s)` | First occurrence index (1-based, like PG) | 1 | Extremely common |
| `left(s, n)` / `right(s, n)` | `left`, `right` | First/last `n` characters | 1 | Trivial, high frequency |
| `split_part(s, delim, n)` | `split_part` | nth delimited fragment | 1 | CSV/compound-key splitting |
| `repeat(s, n)` | `repeat` | Repeat a string `n` times | 1 | |
| `reverse(s)` | `reverse` | Reverse a string | 1 | Supersedes the `reverse` UDF used in the server tests once built-in |
| `lpad(s, len)` / `rpad(s, len)` | `lpad`, `rpad` | Pad to a length (optional pad char) | 2 | |
| `initcap(s)` | `initcap` | Capitalize each word | 2 | |
| `md5(s)` | `md5` | MD5 hex digest | 2 | Tokenization/sanitization |
| `translate(s, from, to)` | `translate` | Char-by-char substitution | 3 | |
| `ascii(ch)` / `chr(code)` | `ascii`, `chr` | Char ↔ code point | 3 | |
| `format(fmt, ...)` | `format` | `printf`-style templating | 3 | Speculative formatting |
| `sha256(s)` (family) | `pgcrypto` `digest` | Cryptographic hashes | 3 | Extension territory |

### 3.3 Numeric and math

| Proposed Ickle name | PostgreSQL counterpart | Purpose | Pri | Notes |
|---|---|---|---|---|
| `sign(x)` | `sign` | −1 / 0 / 1 | 1 | |
| `exp(x)` | `exp` | eˣ | 1 | |
| `ln(x)` | `ln` | Natural logarithm | 1 | |
| `log(base, x)` | `log(b, x)` | Logarithm base b | 1 | |
| `round(x, dp)` / `trunc(x, dp)` | `round(numeric, int)`, `trunc` | Round/truncate to `dp` decimals | 2 | Extends the existing single-argument `round`; keep Java half-up rounding, not PG's half-to-even |
| `div(a, b)` | `div` | Integer division as a function | 2 | Convenience; `/` already truncates for integrals |
| `greatest(v1, v2, ...)` / `least(...)` | `greatest`, `least` | Variadic min/max | 2 | Existing `min`/`max` are two-argument |
| `nullif(a, b)` | `nullif` | `a` unless equal to `b`, then `null` | 2 | |
| `mod(a, b)` | `mod` | Alias of `%` | 3 | Symmetry with PG |
| `gcd(a, b)` / `lcm(a, b)` | `gcd`, `lcm` (PG 14) | Greatest common divisor / least common multiple | 3 | |
| `width_bucket(x, a, b, n)` | `width_bucket` | Bucket a value into ranges | 3 | Analytics niche |
| `random()` | `random()` | Uniform random double in [0,1) | 3 | Non-deterministic — document that parallel plans may diverge |

### 3.4 Conversion and casting

| Proposed Ickle name | PostgreSQL counterpart | Purpose | Pri | Notes |
|---|---|---|---|---|
| `coalesce(a, b, ...)` | `coalesce` | First non-null of a list | 1 | High value for nullable-field defaults; variadic |
| `to_char(ts, fmt)` | `to_char` | Format a temporal value to a string | 3 | Pattern syntax decision needed (ISO vs PG templates) |
| `to_number(s, fmt)` | `to_number` | Parse a formatted number string | 3 | |
| `CAST(x AS TYPE)` | `CAST(... AS ...)` | Explicit cast expression | 3 | **Grammar change** (new production + visitor + AST node) — out of scope for function-only phases |

### 3.5 Conditional expressions (grammar-level, deferred)

| Proposed Ickle name | PostgreSQL counterpart | Purpose | Pri | Notes |
|---|---|---|---|---|
| `CASE WHEN ... THEN ... ELSE ... END` | `CASE` | Conditional value | 3 | Grammar change (multi-statement expression); largest single item on this list |
| `if(cond, a, b)` | (MySQL `IF`) | Ternary shortcut | 3 | Cheaper alternative to `CASE`; non-standard |

---

## 4. Deliberately out of scope

- **Aggregate functions** (`count`, `sum`, `avg`, `string_agg`, `array_agg`,
  `percentile_cont`/`percentile_disc`, `rank`): belong to the
  SELECT/GROUP BY/HAVING subsystem, not to value expressions.
- **Window functions** (`OVER (...)`, frame specifications): SELECT-only.
- **JSON functions** (`json_extract`, `jsonb` operators, `row_to_json`): the
  JSON module's domain; separate feature.
- **Cast expressions** as grammar (see 3.4) until there is demand.
- **Interval literals** (`INTERVAL '3 days'`): requires a new literal type in
  the lexer/parser; the `dateadd`-style numeric-unit functions cover the
  common cases meanwhile.
- **`pgcrypto` bitwise/secret-key functions**: extension territory.

## 5. Prioritized backlog

- **Tier 1 (next phase candidate)**: `now`, `extract`/`date_part`,
  `date_trunc`, `strpos`, `left`, `right`, `split_part`, `repeat`, `reverse`,
  `coalesce`, `sign`, `exp`, `ln`, `log`.
- **Tier 2**: `age`, `to_timestamp`, `to_date`, `lpad`/`rpad`, `initcap`,
  `md5`, `round(x, dp)`/`trunc(x, dp)`, `div`, `greatest`/`least`, `nullif`.
- **Tier 3**: everything else in section 3, plus the conditional-expression
  grammar work.

## 6. Estimate

Each plain function ≈ implementation + `@QueryFunctionSignature` + unit/e2e
tests + one documentation row (roughly 0.5–1 person-day each, most smaller).
Tier 1 ≈ 2–3 weeks of developer time spread over a future phase; Tier 2 ≈
similar; `CASE WHEN` alone is a multi-week grammar + AST + evaluator effort
and should be scoped independently.
