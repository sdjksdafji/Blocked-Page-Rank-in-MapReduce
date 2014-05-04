Blocked-Page-Rank-in-MapReduce
==============================

The project provides 3 method to calculate page rank of a node: simple method, blocked page rank by Gauss-Seidel, or blocked page rank by Jacobi.

3 arguments are needed for running this program.
[use Simple Method or Blocked Gauss-Seidel or Blocked Jacobi: <simple, gauss, jacob>]
[input file or directory: you need to use s3n protocol]
[home directory of the job in s3n protocol]

The net id we used is "yw598".

Gerenal Architecture:

"driver" package contains hadoop driver, global counter, and configuration information.

"pageranck" package contains all mappers and reducers including these for pre and post processing purposes.

Mapper and Reducer functionality:

"CaclulateTotalNumberOfNodeMapper" and "CaclulateTotalNumberOfNodeReducer" filters the edge then account the number of unique nodes and returns the number by global counter.

"FormatInputMapper" and "FormatInputReducer" filters and refomats the edge file.

"BlockedPageRankMapper", "BlockedPageRankReducer", "SimplePageRankMapper", and "SimplePageRankReducer" implments the algorithm iteration.

"FormatOuputMapper" and "FormatOutReducer" cleans and formats the calculation results for output.


