package tg.dtg.main.examples;

import com.beust.jcommander.Parameter;

public class Argument {
  @Parameter(names = {"-i","--inputEvent"}, description = "input event path", required = true)
  String path;

  @Parameter(names = {"-wl"}, description = "window length")
  long wl;

  @Parameter(names = {"-sl"}, description = "window slide")
  long sl;

  @Parameter(names = {"-p","--parallism"}, description = "parallism")
  int parallism = -1;
}
