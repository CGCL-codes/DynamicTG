package tg.dtg.graph.detect.traversal.anchors;

import static tg.dtg.util.Global.getParallism;
import static tg.dtg.util.Global.runAndSync;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import tg.dtg.cet.EventTrend;
import tg.dtg.cet.SimpleEventTrend;

public class AnchorGraph {

  private final ArrayList<AnchorVertex> startVertices;
  private final AnchorVertex endVertex;
  private final Function<Collection<AnchorVertex>,
      HashSet<AnchorVertex>> selectAnchors;
  private final Consumer<EventTrend> outputFunc;
  private HashSet<AnchorVertex> anchors;

  public AnchorGraph(HashSet<AnchorVertex> anchors,
      ArrayList<AnchorVertex> startVertices,
      AnchorVertex endVertex,
      Function<Collection<AnchorVertex>,
          HashSet<AnchorVertex>> selectAnchors,
      Consumer<EventTrend> outputFunc) {
    this.anchors = anchors;
    this.startVertices = startVertices;
    this.endVertex = endVertex;
    this.selectAnchors = selectAnchors;
    this.outputFunc = outputFunc;
  }

  public AnchorGraph(HashSet<AnchorVertex> anchors,
      ArrayList<AnchorVertex> startVertices,
      AnchorVertex endVertex,
      Consumer<EventTrend> outputFunc) {
    this(anchors, startVertices, endVertex, null, outputFunc);
  }

  public void detectOnePredicate(int numIteration) {
    Preconditions.checkArgument(numIteration > 0);
    try {
      anchors.remove(endVertex); // avoid selecting end vertex
      while (numIteration > 0) {
        HashSet<AnchorVertex> newAnchors = selectAnchors.apply(anchors);
//        newAnchors.addAll(startVertices);
        ArrayList<Runnable> tasks = new ArrayList<>(newAnchors.size());
        for (AnchorVertex vertex : newAnchors) {
          tasks.add(() -> doBFS(vertex, newAnchors));
        }
        runAndSync(tasks);
        //doBFS(startVertex,newAnchors);
        anchors = newAnchors;
        numIteration--;
      }
      anchors.add(endVertex);
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void doBFS(AnchorVertex start, HashSet<AnchorVertex> isAnchors) {
    Collection<TrendVertex> vertices = start.getEdges();
    HashSet<TrendVertex> newVertex = new HashSet<>();

    do {
      HashSet<TrendVertex> nextLayer = new HashSet<>();
      for (TrendVertex vertex : vertices) {
        Iterator<AnchorVertex> edgesIt = vertex.getEdges().iterator();
        HashSet<TrendVertex> outer = new HashSet<>();
        while (edgesIt.hasNext()) {
          AnchorVertex anchor = edgesIt.next();
          if (anchor == this.endVertex) {
            if (start.isStart()) {
              // from start to end, output
              outputFunc.accept(vertex.eventTrend);
            } else {
              //List<EventTrend> eventTrends = trends.get(vertex);
              newVertex.add(vertex);
            }
          } else if (!isAnchors.contains(anchor)) {
            // not anchor, traverse its trend vertices
            edgesIt.remove();
            for (TrendVertex v : anchor.getEdges()) {
              if (v.eventTrend.start() > vertex.eventTrend.end()) {
                outer.add(v);
              }
            }
          } else {
            // anchor, make new trend vertices and connect with edges
            newVertex.add(vertex);
          }
        }
        for (TrendVertex tv : outer) {
          TrendVertex nv = vertex.copyAndAppend(tv);
          nextLayer.add(nv);
        }
        //nextLayer.addAll(outer);
      }
      vertices = nextLayer;
    } while (!vertices.isEmpty());
    start.newEdges(newVertex);
  }

  public void computeResults() {
    for(AnchorVertex startVertex: startVertices) {
      for (TrendVertex trendVertex : startVertex.getEdges()) {
        for (AnchorVertex anchorVertex : trendVertex.getEdges()) {
          if (anchorVertex == endVertex) {
            outputFunc.accept(trendVertex.eventTrend);
          } else {
            doDFS(anchorVertex, trendVertex.eventTrend);
          }
        }
      }
    }
  }

  private void doDFS(AnchorVertex anchor, EventTrend trend) {
    for (TrendVertex trendVertex : anchor.getEdges()) {
      if (trendVertex.eventTrend.start() <= trend.end()) {
        continue;
      }
      ArrayList<AnchorVertex> edges = trendVertex.getEdges();
      int i = 0;
      for (; i < edges.size() - 1; i++) {
        AnchorVertex anchorVertex = edges.get(i);
        EventTrend eventTrend = trend.copy();
        eventTrend.append(trendVertex.eventTrend);
        if (anchorVertex == endVertex) {
          outputFunc.accept(eventTrend);
        } else {
          doDFS(anchorVertex, eventTrend);
        }
      }
      // avoid one copy
      if (i < edges.size()) {
        AnchorVertex anchorVertex = edges.get(i);
        EventTrend eventTrend = trend.copy();
        eventTrend.append(trendVertex.eventTrend);
        if (anchorVertex == endVertex) {
          outputFunc.accept(eventTrend);
        } else {
          doDFS(anchorVertex, eventTrend);
        }
      }
    }
  }

  public void doParallelDFS() {
    ArrayList<StealBatchDFSTask> tasks = new ArrayList<>();
    AtomicInteger index = new AtomicInteger();
    ArrayBlockingQueue<StealBatchDFSTask> idleTasks = new ArrayBlockingQueue<>(getParallism()*2);

    for (int i = 0; i < getParallism(); i++) {
      tasks.add(new StealBatchDFSTask(index, startVertices, endVertex, outputFunc, idleTasks));
    }
    ArrayList<Runnable> runnables = new ArrayList<>();
    for (StealBatchDFSTask task : tasks) {
      task.setOtherTasks(tasks);
      runnables.add(task);
    }
    try {
      runAndSync(runnables);
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void output(List<SimpleEventTrend> eventTrends) {
    eventTrends.forEach(outputFunc);
  }

  private static class StealBatchDFSTask implements Runnable{

    private static int taskID = 0;
    private final AnchorVertex endVertex;
    private final Consumer<EventTrend> outputFunc;
    private final int taskId;
    private final ArrayBlockingQueue<StealBatchDFSTask> idleTasks;
    private List<Vertex> path;
    private AtomicInteger startIndex;
    private ArrayList<AnchorVertex> startVertices;
    private Vertex cur;
    private Thread thread;
    /**
     * status: 0 -> init status
     * 1 -> normal work
     * 2 -> waiting for task distribute
     * 100 -> complete
     */
    private int status;
    private List<StealBatchDFSTask> otherTasks;

    public StealBatchDFSTask(AtomicInteger startIndex, ArrayList<AnchorVertex> startVertices,
        AnchorVertex endVertex, Consumer<EventTrend> outputFunc,
        ArrayBlockingQueue<StealBatchDFSTask> idleTasks) {
      this.startIndex = startIndex;
      this.startVertices = startVertices;
      this.outputFunc = outputFunc;
      this.endVertex = endVertex;
      this.idleTasks = idleTasks;
      path = new ArrayList<>();
      status = 0;
      taskId = StealBatchDFSTask.taskID;
      StealBatchDFSTask.taskID++;
    }

    public void setOtherTasks(
        List<StealBatchDFSTask> tasks) {
      this.otherTasks = new ArrayList<>();
      otherTasks.addAll(tasks);
      otherTasks.remove(this);
    }

    public int getStatus() {
      return status;
    }

    @Override
    public void run() {
      thread = Thread.currentThread();
      status = 1;
      while (true){
        int i = startIndex.getAndIncrement();
        if(i < startVertices.size()) {
          cur = startVertices.get(i);
          doDFS();
        }else break;
      }
      int lstatus;
      while (true){
        lstatus = steal();
        if (lstatus < 0) {
          System.out.println("1.pdfs " + taskId + " complete");
          status = 100;
          return;
        }else{
          try {
            doDFS();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    }

    private int steal() {
      //System.out.println(taskId + " want to steal a work");
      status = 2;
      idleTasks.offer(this);
      try {
        while (status == 2) {
          TimeUnit.MICROSECONDS.sleep(500);
          boolean hasWorkTask = false;

          for (StealBatchDFSTask otherTask : otherTasks) {
            if (otherTask.status == 1) {
              hasWorkTask = true;
              break;
            }
          }
          if (!hasWorkTask) {
            return -1;
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return 1;
    }

    private SimpleEventTrend extractPath() {
      int i = 0;
      if (path.get(0) instanceof AnchorVertex) {
        i = 1;
      }
      SimpleEventTrend simpleEventTrend = new SimpleEventTrend();
      for (; i < path.size(); i += 2) {
        simpleEventTrend.append(((TrendVertex) (path.get(i))).eventTrend);
      }
      return simpleEventTrend;
    }

    private void doDFS() {
      StealBatchDFSTask idelTask;
      if ((idelTask = idleTasks.poll()) !=null) {
        idelTask.path.clear();
        idelTask.path.addAll(this.path);
        idelTask.cur = this.cur;
        return;
      }
      Vertex localCur = cur;
      if (localCur instanceof TrendVertex) { // cur is a trend
        for (Vertex vertex : localCur.cedges()) {
          if (vertex == endVertex) {
            outputFunc.accept(extractPath());
          } else {
            path.add(vertex);
            cur = vertex;
            doDFS();
            path.remove(path.size() - 1);
            cur = localCur;
          }
        }
      } else {                                   // cur is a anchor
        long curtm = -1;
        if (path.size() > 1) {
          curtm = ((TrendVertex) (path.get(path.size() - 2))).eventTrend.end();
        }
        for (Vertex vertex : localCur.cedges()) {
          if (((TrendVertex) vertex).eventTrend.start() <= curtm) {
            continue;
          }
          cur = vertex;
          path.add(vertex);
          doDFS();
          path.remove(path.size() - 1);
          cur = localCur;
        }
      }
    }

    @Override
    public String toString() {
      return "BatchDFSTask{" +
          "taskId=" + taskId +
          ", status=" + status +
          ", num_idles" + idleTasks.size() +
          ", curThread=" + thread +
          '}';
    }
  }
}
