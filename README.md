# Flume Headers Enrichment Interceptor

This flume interceptor populate the event headers, crossing data between the Event Headers Information an a provided CSV File

Compilation and packaging
----------
```
  $ mvn package
```

Deployment
----------

Copy headerEnrichmentInterceptor-<version>.jar in target folder into flume plugins dir folder
```
  $ mkdir -p $FLUME_HOME/plugins.d/headerEnrichmentInterceptor/lib
  $ cp headerEnrichmentInterceptor-0.0.1.jar $FLUME_HOME/plugins.d/headerEnrichmentInterceptor/lib
```

Configuration
----------
Required properties in bold

| Property name | Default value | Description
| ----------------------- | :-----: | :---------- |
| <b>csvHeadersFile</b>| null | Where are stored the enrichment CSV file
| <b>matchHeaderKey</b> | null | What header from the Flume event will be used to do the matching
| csvSeparatorChar | , | CSV separator character

Providing the CSV File
------
<p>The first line of the csv file, must contain the field names, and the first value name have to be "mathHeaderValue"</p>

Example!
---------------------
With the a CSV File in /home/flume/enrichment/enrichmentFile.csv like
```
matchHeaderValue,Country,City,latitude,longitude
10.12.3.21,Spain,Sevilla,37.392529,-5.994072
```
And the interceptor config
```apacheconf
a1.sources = s1
a1.channels = c1

a1.sources.s1.type = syslogtcp
a1.sources.s1.host = 0.0.0.0
a1.sources.s1.port = 5140
a1.sources.s1.channels = c1

a1.sources.s1.interceptors = i1
a1.sources.s1.interceptors.i1.type = org.keedio.flume.interceptor.HeaderEnrichmentInterceptor$Builder
a1.sources.s1.interceptors.i1.csvHeadersFile = /home/flume/enrichment/enrichmentFile.csv
a1.sources.s1.interceptors.i1.matchHeaderKey = host
```

When the following Flume event pass throug the interceptor:
```
Event Headers --> [{Facility=[22], Severity=[6], host=[10.12.3.21], priority=[182], timestamp=[1480944606164]}]
Event Body --> [Flume event body test]
```
The output event will be:
```
Event Headers --> [{Facility=[22], Severity=[6], host=[10.12.3.21], priority=[182], timestamp=[1480944606164]},
                   Country=[Spain],City=[Sevilla],latitude=[37.392529],longitude=[-5.994072]]
Event Body --> [Flume event body test]
```
