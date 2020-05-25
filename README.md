#DynamicTG
A Complete Event Trend (CET) detection system.

### dependencies
```
  com.beust:jcommander:1.78
  com.google.guava:guava:28.1-jre
  com.typesafe:config:1.4.0
```

### usage
Currently we mainly implement two examples in the package `tg.dtg.main.examples`. You can run these examples with the command:
```bash
java -jar $jarfile $type [options]
```
in which `$type$` is the name of example.

For other applications, you can extend `tg.dtg.main.examples.Example` and customize by yourself.

### Options
`-i file`

the event input file.

`-wl wl,-sl sl`

the window length and slide length

`-p parallism`

how much threads are used. `-1` means use sequential algorithm.

`-sel step`

the step length for select anchors. Default is 2. If `step < 0`, the extraction processs is omitted.

`-pdfs`

[Optional] Whether a parallel dfs-based method is applied.

`-w dir`

[Optional] the directory that the edges and vertices are written to. For debug.

`-out`

[Optional] if set, the final CETs result will be written to the directory that are set by `-w`


