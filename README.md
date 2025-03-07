
`ol-diff` is a tool to compare lineage events from different producers versions producing OpenLineage events for the same job.
It makes sure that events produced by a previous version are reflected in the next version. 
This can be useful when upgrading OpenLineage producer with custom changes implemented. 

`ol-diff` takes two sets of OpenLineage events, compares them and outputs the result into a report. 

Simple example usage:
```
./ol-diff.sh
```

TODO: 
 * support extracting events from logs when ConsoleTransport enabled
 * write documentation
 * 