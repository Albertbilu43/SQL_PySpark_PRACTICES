





\---------------------------------Source: https://www.youtube.com/watch?v=3BrL5S8Xg2s--------------------------------



In the SQL logical processing order, the SELECT clause is processed 8th (near the end). Even though it appears first in written syntax, the database engine executes it after data filtering and grouping are complete.



The Complete Logical Processing Order



The Microsoft Learn documentation clarifies that the database engine evaluates clauses in this specific sequence:



1. FROM: Locates tables and processes JOIN constraints.

2. ON: Applies specific join conditions.

3. JOIN: Merges datasets together.

4. WHERE: Filters rows matching base conditions.

5. GROUP BY: Bundles remaining rows into groups.

6. WITH CUBE / WITH ROLLUP: Generates extra super-aggregate rows.

7. HAVING: Filters groups based on aggregate conditions.

8. SELECT: Evaluates expressions, selects columns, and assigns column aliases.

9. DISTINCT: Eliminates duplicate rows from the results.

10. ORDER BY: Sorts final rows by specified expressions.

11. TOP / OFFSET-FETCH: Restricts the final row count

