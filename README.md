# nagios-nlsexport
Export all of your Nagios Log Server data, or only some of it, with this handy application.
```
Version - 1.3.0
Usage: java -jar nlsexport.jar -host -date_start -date_end -output_path [-output_format] [-query] [-types]
host - The hostname or ip address of the remote Elasticsearch machine. Your Elasticsearch API must be front-facing for this application to work.
date_start - The starting date of your data set in yyyy.mm.dd format.
date_end - The ending date of your data set in yyyy.mm.dd format.
output_path - The path that will contain your data. Many individual files are created, so it is recommended to use a dedicated path.
output_format - OPTIONAL (json default), the format you would like your data to be saved in. Acceptable options: csv, json, text. The text option will only export the message field (if it exists).
query - OPTIONAL (no default), the Elasticsearch query you would like to use to filter your results.
types - OPTIONAL (no default), the Elasticsearch types you would like to include in your result set as a comma-delimited list. This is useful if you only want to include certain types of events (syslog, eventlog, apache_access, etc) without including that information in your query's logic. Including the type in your query logic can be inefficient at times.
------------------------------------------
Sample: java -jar nlsexport.jar -host=192.168.0.1 -date_start=2015.01.01 -date_end=2015.12.31 -output_path=/home/juser/export_nls/ -output_format=csv
----- Exports all data from the year 2015 into CSV files into the /home/juser/export_nls/ path.
Sample: java -jar nlsexport.jar -host=localhost -date_start=2015.01.01 -date_end=2015.12.31 -output_path=/home/juser/export_nls/ -query='{"query":{"query_string":{"query":"my query string"}}}'
----- Exports all entries (given the query) from the year 2015 into raw JSON (default) into the /home/juser/export_nls/ path.
Sample: java -jar nlsexport.jar -host=localhost -date_start=2016.01.01 -date_end=2016.12.31 -output_path=/home/juser/export_nls/ -types=syslog,eventlog
----- Exports all syslog and eventlog entries from the month of December 2016 into raw JSON (default) into the /home/juser/export_nls/ path.

```
# Version - 1.3.0
* No longer creates a directory when provided invalid/incomplete arguments
* Added the text output, which exports the message field as plain text
* Added better handling of malformed JSON, tells you more information about why the JSON is malformed
* Added -types argument, which accepts a comma-delimited list of types you wish to export. This is much more efficient than including specific types as a filtered query
* Cleaned up the code a bit

# Version - 1.2.4
* Fixed an issue with nested JsonObjects throwing UnsupportedOperationException

# Version - 1.2.3
* Fixed an issue with weird Elasticsearch types causing NPEs

# Version - 1.2.2
* Fixed an issue with exporting all types, some were being excluded
* Fixed an issue with NullPointerException and the _default_ type

# Version - 1.2.1
* Fixed an issue with CSV formatting
* Fixed an issue with command-line args having spaces in them

# Version - 1.2.0
* Added CSV output
* Better command-line argument processing

# Version - 1.1.0
* Initial release
