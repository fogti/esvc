# event sourcing version control

This is the worst (performance-wise) version control software I can imagine.
Yes, afaik worse than commomly used blockchains and Darcs.
The primary functionality is "merging" parallel event branches,
and that is slow (O(n²) where n is the number of parallel branches).

This also means that this can be only used on really small datasets,
and because of that, I made the library simpler by requiring that
the data set is completely present in memory while working on it
(as in: no file system access while modifying it).
