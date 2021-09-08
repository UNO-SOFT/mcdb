# Multi-CDB
Write Once Read Many Constant Database.
It wraps github.com/colinmarc/cdb 
(See the original cdb specification and C implementation by D. J. Bernstein at http://cr.yp.to/cdb.html)
to hide the 4GiB size limit.

The database(s) can be written only once, and you must set the number of tables to use beforehand,
or use the automatic grow facility (see Writer.Put).

## Rationale
CDB is fast and very simple. For read-only access, it's ideal.
The size limit is too low nowadays.

## CLI
./cmd/cdb is a command-line program that can dump in cdbmake-format, and load from that format.

## Versions
### v0 - no versioning
Uses FNV for bucket distribution, and FNV for CDB hashing.

### v1 - version in filename
Uses FNV for bucket distribution, and the default CDB hashing.
