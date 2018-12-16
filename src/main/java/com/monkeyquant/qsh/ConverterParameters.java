package com.monkeyquant.qsh;

import com.monkeyquant.qsh.model.BarPeriod;
import com.monkeyquant.qsh.model.OutputFileType;
import lombok.Getter;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.BooleanOptionHandler;

@Getter
class ConverterParameters {
  @Option(name = "-infile", usage = "input filename", required = true)
  private String inputFile;

  @Option(name = "-outfile", usage = "output filename")
  private String outputFile;

  @Option(name = "-type", usage = "output file type", required = true)
  private OutputFileType outputFileType;

  @Option(name = "-period", usage = "period for BARS file type", depends = {"-type=BARS"})
  private BarPeriod barPeriod;

  @Option(name = "-usebook", handler = BooleanOptionHandler.class, usage = "use book states for bars collect instead of ticks")
  private Boolean useBookState = false;

  @Option(name = "-timeFormat", usage = "time and date format")
  private String timeFormat;

  @Option(name = "-timeQuant", usage = "time quant in msec")
  private Integer timeQuant;

  @Option(name = "-mql", handler = BooleanOptionHandler.class, usage = "use MQL tick or bar format")
  private Boolean useMql = false;

  @Option(name = "-scale", usage = "scale for prices")
  private Integer scale = 2;

  @Option(name = "-start", usage = "start time, default=600")
  private Integer start = 600;

  @Option(name = "-end", usage = "end time, default=1425")
  private Integer end = 1425;

}
