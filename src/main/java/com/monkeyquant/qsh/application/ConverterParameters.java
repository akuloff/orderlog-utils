package com.monkeyquant.qsh.application;

import com.monkeyquant.qsh.model.BarPeriod;
import com.monkeyquant.qsh.model.OutputFormatType;
import com.monkeyquant.qsh.model.TimeFilter;
import com.monkeyquant.qsh.model.TimeOfBar;
import lombok.Getter;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.BooleanOptionHandler;

@Getter
public class ConverterParameters {
  @Option(name = "-infile", usage = "input filename", required = true)
  private String inputFile;

  @Option(name = "-outfile", usage = "output filename")
  private String outputFile;

  @Option(name = "-type", usage = "output format type", required = true)
  private OutputFormatType outputFormatType;

  @Option(name = "-period", usage = "period for BARS file type", depends = {"-type=BARS"})
  private BarPeriod barPeriod;

  @Option(name = "-bartime", usage = "source for bar time")
  private TimeOfBar barTime = TimeOfBar.close;

  @Option(name = "-usebook", handler = BooleanOptionHandler.class, usage = "use book states for bars collect instead of ticks")
  private Boolean useBookState = false;

  @Option(name = "-dateFormat", usage = "time and date format")
  private String dateFormat;

  @Option(name = "-timeFormat", usage = "time and date format")
  private String timeFormat;

  @Option(name = "-timeQuant", usage = "time quant in msec for BOOKSTATE")
  private Integer timeQuant;

  @Option(name = "-mql", handler = BooleanOptionHandler.class, usage = "use MQL tick or bar format")
  private Boolean useMql = false;

  @Option(name = "-saveTradeId", handler = BooleanOptionHandler.class, usage = "save trade ID to TICKS data")
  private Boolean saveTradeId = false;

  @Option(name = "-noheader", handler = BooleanOptionHandler.class, usage = "do not write header for columns")
  private Boolean noHeader = false;

  @Option(name = "-closeonly", handler = BooleanOptionHandler.class, usage = "write close only price in OHLC bars")
  private Boolean closeOnly = false;

  @Option(name = "-writezero", handler = BooleanOptionHandler.class, usage = "write zero ask or bid for BOOKSTATE output")
  private Boolean writeZero = false;

  @Option(name = "-scale", usage = "scale for prices")
  private Integer scale = 2;

  @Option(name = "-start", usage = "start time, default=600")
  private Integer start = 600;

  @Option(name = "-end", usage = "end time, default=1425")
  private Integer end = 1425;

  @Option(name = "-timefilter", usage = "market time filter, default = NONE")
  private TimeFilter timeFilter = TimeFilter.NONE;

  @Option(name = "-batch", handler = BooleanOptionHandler.class, usage = "batch files procesing (from file mask)")
  private Boolean batchProcess = false;

  @Option(name = "-onefile", handler = BooleanOptionHandler.class, usage = "use one file for output (only with batch processing)", depends = {"-batch"})
  private Boolean oneFileProcess = false;
}
