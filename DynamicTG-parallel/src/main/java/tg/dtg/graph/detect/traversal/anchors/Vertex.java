package tg.dtg.graph.detect.traversal.anchors;

import java.util.List;

public interface Vertex {
  List<? extends Vertex> cedges();
}
