# nagios-nlsexport
Export all of your Nagios Log Server data, or only some of it, with this handy application.
```
Usage: java -jar nlsexport.jar host start_date end_date output_path [format] [query]
host - The hostname or ip address of the remote Elasticsearch machine. Your Elasticsearch API must be front-facing for this application to work.
start_date - The starting date of your data set in yyyy.mm.dd format.
end_date - The ending date of your data set in yyyy.mm.dd format.
output_path - The path that will contain your data. Many individual files are created, so it is recommended to use a dedicated path.
format - OPTIONAL (json default), the format you would like your data to be saved in. Acceptable options: csv, json.
query - OPTIONAL (no default), the Elasticsearch query you would like to use to filter your results.
------------------------------------------
Sample: java -jar nlsexport.jar 192.168.0.1 2015.01.01 2015.12.31 /home/juser/export_nls csv
----- Exports all data from the year 2015 into CSV files into the /home/juser/export_nls path.
Sample: java -jar nlsexport.jar localhost 2015.01.01 2015.12.31 /home/juser/export_nls '{"query":{"query_string":{"query":"syslog"}}}'
----- Exports all syslog entries (given the query) from the year 2015 into raw JSON (default) into the /home/juser/export_nls path.
```
# Version - 1.1.0
* Initial release