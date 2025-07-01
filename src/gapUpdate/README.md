# Overview
This function prosesses batches of event records each representing a granule's period. For each batch,
it calculates the difference of sets of time intervals to maintain the gap table, which records intervals
representing the abscense of observation for each collection's entire duration. 
Records in the gap table consist of the half-open interval [start, end), meaning the gap
includes start and ends before end. This is different from the representation used by granules,
which is a closed interval that includes both endpoints. 

## Testing
Tests can be run and coverage reported by running the test script from the directory root:
`./test.sh`
Tests are currently incomplete and consist only of a skeleton.

## Deployment
A Dockerfile that installs the dependencies and produces a zip archive is included. This can
be used in Bamboo for packaging.
