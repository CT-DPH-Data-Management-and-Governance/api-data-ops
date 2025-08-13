## V0.8.0

> Note: This package is in active development, expect breaking changes.

### Breaking Changes
- Package, subpackage and modules all reorganized so namespaces changed 
for most if not all functions and methods.

### Features
- Suite of "tidy" LazyFrame outputs for looking at API output in various ways.
- Label parsing
- Variable ID parsing 
- Concept and Universe pulled if they exist
- Caching of foundational Lazyframes and methods both within pydantic and polars
- Methods with "sane" defaults and standard formats, but also methods
for deeper dives and 

### Minor
- `measure_id` added to some outputs to group the multiple line suffixes (e.g. E, EA, M etc...)
that belong to a singular measure.
- Unified raw request handling and lazyframe creation for easier DX.