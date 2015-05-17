CSE562 - Database Systems
=======

This project implements a SQL-to-Relational Algebra query engine. It takes SQL files and .dat data files (CSV files containing data tuples) as input. The entire project consists of modules like:

1. Query Parser:
   - parses SQL SELECT queries.
   - parses SQL CREATE queries and generates schema metadata for each relation (table) created.

2. Query Loader:
   - loads a SQL query into a Relational Algebra tree (RA-tree) structure.

3. Optimizer:
   - optimizes a given RA-tree by recursively pushing down selection predicates, optimizing joins and other such operations.

4. Query Planner:
   - based on the inputs from Query Loader and Optimizer, generates an optimized query plan for each query.
   - executes the query as per plan by calling the required operators like Scan, Selection, Projection, Aggregation and so on.

The project supports the following SQL operators:

    - Scan (FROM)

    - Selection (WHERE)

    - Projection (SELECT)

    - Aggregation (GROUP BY)

    - ORDER BY (ascending and descending)

    - LIMIT
