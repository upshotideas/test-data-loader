# Test Data Loader
Loads data from csv files into a given database, with simple controls for clearing or reloading data for all your end-to-end test case scenarios.



# TODOs:
1. Not able to handle if there are single quotes in the csv.
2. Not able to handle if the data contains double quotes.
3. In general needs better csv handling.
4. Cannot handle changed column order, or column mismatch between table and csv.
5. Improve insert performance by using pagination of sorts, grouping values.
6. Try using init script for h2: INIT=RUNSCRIPT from 'createTables.sql';
7. Change to numbers for sorting, seems we are using strings here, it will create issues when we have more than 10 tables.
8. Should we handle order or delete, just to support foreign keys? Or disable the key validations before clear.
9. Find a way to remove the reflection based test to check natural order sorting.
10. CSV handling: Handle cases where blank columns cause invalid sql issues. Ex: handle `... values(1,,,,)` by adding NULL as column value as `... values(1,NULL,NULL,NULL,NULL)`.
