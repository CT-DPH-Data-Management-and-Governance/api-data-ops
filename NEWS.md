> Note: This package is in active development, expect breaking changes.

## v0.10.0
- standardize text and repleace "measure" with "health indicator" to better
mirror world health organization language.

## v0.9.1

### Fixes
- {builders}: add the variable id from the API back into the fact as `acs_variable`.

## v0.9.0

### Features

- Breaking: Updated minimum python version to 3.13.6
- Added `builder` subpackage with builder methods for creating a star model
in memory.


## v0.8.0

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