package tg.dtg.main;

import tg.dtg.main.examples.Example;

public class Main {

  public static void main(String[] args) {
    Example example = Example.getExample(args);

    example.start();
  }

}
