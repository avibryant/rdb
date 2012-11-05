rdb

Simple Java library for writing out Redis .rdb dump files.

So far supports:
- strings, lists, sets, hash tables
- compact encoding of integer strings

- no sorted sets, no fancy ziplist etc encodings of sets and hashes
- compressed string encoding most of the way there but not working yet
- no key expiry

Avi Bryant