package gelly.graphalytics;

import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import nl.tudelft.graphalytics.flink.algorithms.bfs.ScatterGatherBFS;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.algorithms.bfs.BreadthFirstSearchOutput;
import nl.tudelft.graphalytics.validation.algorithms.bfs.BreadthFirstSearchValidationTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.util.*;

public class BFSJobTest extends BreadthFirstSearchValidationTest {
    @Override
    public BreadthFirstSearchOutput executeDirectedBreadthFirstSearch(
            GraphStructure graphStructure, BreadthFirstSearchParameters params)
            throws Exception {

        Graph<Long, NullValue, NullValue> input = getInputGraph(graphStructure, false);
        // run the BFS job
        DataSet<Tuple2<Long, Long>> result = input.run(new ScatterGatherBFS(params));
        return convertResult(result);
    }

    @Override
    public BreadthFirstSearchOutput executeUndirectedBreadthFirstSearch(
            GraphStructure graphStructure, BreadthFirstSearchParameters params)
            throws Exception {

        Graph<Long, NullValue, NullValue> input = getInputGraph(graphStructure, false);
        // run the BFS job
        DataSet<Tuple2<Long, Long>> result = input.run(new ScatterGatherBFS(params));
        return convertResult(result);
    }

    // helper method to create the input Gelly Graph from the GraphStructure
    private Graph<Long, NullValue, NullValue> getInputGraph(GraphStructure graphStructure, boolean undirected) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // get the vertices
        Set<Long> vertexSet = graphStructure.getVertices();

        DataSet<Vertex<Long, NullValue>> vertices = env.fromCollection(vertexSet)
                .map(new MapFunction<Long, Vertex<Long, NullValue>>() {
                    @Override
                    public Vertex<Long, NullValue> map(Long id) {
                        return new Vertex<>(id, NullValue.getInstance());
                    }
                });

        // get the edges
        Set<Edge<Long, NullValue>> edgeSet = new HashSet<>();
        for (Long v: vertexSet) {
            Set<Long> neighbors = graphStructure.getEdgesForVertex(v);
            for (Long n: neighbors) {
                edgeSet.add(new Edge<>(v, n, NullValue.getInstance()));
                if (undirected) {
                    edgeSet.add(new Edge<>(n, v, NullValue.getInstance()));
                }
            }
        }

        DataSet<Edge<Long, NullValue>> edges = env.fromCollection(edgeSet);
        // create the graph
        return Graph.fromDataSet(vertices, edges, env);
    }

    // convert the Gelly result to the expected result
    private BreadthFirstSearchOutput convertResult(DataSet<Tuple2<Long, Long>> result) throws Exception {
        // convert the result to the expected output
        List<Tuple2<Long, Long>> resList = result.collect();
        Map<Long, Long> bfsResults = new HashMap<>();
        for (Tuple2<Long, Long> t: resList) {
            bfsResults.put(t.f0, t.f1);
        }
        return new BreadthFirstSearchOutput(bfsResults);
    }
}
