package tg.dtg.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class extract {

  public static void main(String[] args) {
    String pats1 = "name: (\\S+).*window: (\\d+), (\\d+).*parallism: (\\d+).*selectivity: (\\d+)";
    String pats2 = "begin graph construction, events (\\d+) time (\\d+).*finish stream time (\\d+)"
        + ".*finish manage time (\\d+).*attrs: (\\d+).*from edges: (\\d+).*to edges: (\\d+)";
    String pats3 = "begin detect time (\\d+).*finish select anchors time (\\d+)"
        + ".*finish prefilter time (\\d+).*finish first iteration time (\\d+)"
        + ".*cet number (\\d+).*finish detect time (\\d+)";
    assert args.length == 2;
    int mode = Integer.parseInt(args[0]);
    String pats = ".*" + pats1;
    if ((mode & 2) != 0) {
      pats = pats + ".*" + pats2;
    }
    if ((mode & 1) != 0) {
      pats = pats + ".*" + pats3;
    }
    pats+=".*";
    System.out.println(pats);

    Pattern pattern = Pattern.compile(pats, Pattern.MULTILINE | Pattern.DOTALL);
    System.out.println(header());
    try (BufferedReader br = new BufferedReader(new FileReader(args[1]))) {
      String s = "init",line;
      while ((line = br.readLine())!=null){
        if(line.startsWith("name: ")) {
          if(!s.equals("init")) {
            System.out.println(s.length());
            Matcher matcher = pattern.matcher(s);
//            System.out.println(s);
            if(matcher.matches()) {
              System.out.println(toString(matcher,mode));
            }
          }
          s=line;
        }else {
          if(!s.equals("init")) {
            s = s + "\n" + line;
          }
        }
      }
      if(!s.equals("init")) {
        Matcher matcher = pattern.matcher(s);
        if(matcher.matches()) {
          System.out.println(toString(matcher,mode));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }


    final static String[] metas = {
        "name",
        "wl",
        "sl",
        "parallism",
        "selectivity",

        // construct
        "numEvents",
        "s1",
        "fsteam",
        "fmanage",
        "numAttrs",
        "numFrom",
        "numTo",

        //detect
        "s2",
        "fanchor",
        "fprefilter",
        "fbfs",
        "numCETs",
        "fdfs",
    };

    static String header(){
      return String.join(",",metas);
    }

    static String toString(Matcher m, int mode) {
      String[] results = new String[metas.length];
      Arrays.fill(results,"-");

      for (int i = 0,j=0; j < m.groupCount();i++) {
        if ((mode & 2) == 0 && i > 4 && i < 12) {
          continue;
        }
        if ((mode & 1) == 0 && i >= 12) {
          continue;
        }
        results[i] = m.group(j+1);
        j++;
      }
      return String.join(",", results);
    }

}
