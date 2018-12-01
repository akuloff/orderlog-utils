package com.monkeyquant.qsh;

import com.monkeyquant.qsh.model.BarPeriod;
import com.monkeyquant.qsh.model.OutputFileType;
import lombok.Getter;
import org.kohsuke.args4j.Option;

@Getter
class ConverterParameters {
  @Option(name = "-infile", usage = "input filename", required = true)
  private String inputFile;

  @Option(name = "-outfile", usage = "output filename")
  private String outputFile;

  @Option(name = "-type", usage = "output file type", required = true)
  private OutputFileType outputFileType;

  @Option(name = "-period", usage = "period for BARS file type")
  private BarPeriod barPeriod;

  @Option(name = "-usebookstate", usage = "use book states for bars collect instead of ticks")
  private Boolean useBookState;

  @Option(name = "-timeFormat", usage = "time and date format")
  private String timeFormat;

  @Option(name = "-timeQuant", usage = "time quant in msec")
  private Integer timeQuant;

}
