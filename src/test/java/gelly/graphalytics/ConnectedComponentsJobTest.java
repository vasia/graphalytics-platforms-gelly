package gelly.graphalytics;

import nl.tudelft.graphalytics.flink.algorithms.wcc.ScatterGatherConnectedComponents;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.algorithms.wcc.WeaklyConnectedComponentsOutput;
import nl.tudelft.graphalytics.validation.algorithms.wcc.WeaklyConnectedComponentsValidationTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.util.*;

public class ConnectedComponentsJobTest extends WeaklyConnectedComponentsValidationTest {

    @Override
    public WeaklyConnectedComponentsOutput executeDirectedConnectedComponents(
            GraphStructure graphStructure) throws Exception {

        Graph<Long, NullValue, NullValue> input = getInputGraph(graphStructure);
        // run the WCC job
        DataSet<Tuple2<Long, Long>> result = input.run(new ScatterGatherConnectedComponents(true));
        return convertResult(result);
    }

    @Override
    public WeaklyConnectedComponentsOutput executeUndirectedConnectedComponents(
            GraphStructure graphStructure) throws Exception {

        Graph<Long, NullValue, NullValue> input = getInputGraph(graphStructure);
        // run the WCC job
        DataSet<Tuple2<Long, Long>> result = input.run(new ScatterGatherConnectedComponents(false));
        return convertResult(result);
    }

    // helper method to create the input Gelly Graph from the GraphStructure
    private Graph<Long, NullValue, NullValue> getInputGraph(GraphStructure graphStructure) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // get the vertices
        Set<Long> vertexSet = graphStructure.getVertices();

        // get the edges
        Set<Edge<Long, NullValue>> edgeSet = new HashSet<>();
        for (Long v: vertexSet) {
            Set<Long> neighbors = graphStructure.getEdgesForVertex(v);
            for (Long n: neighbors) {
                edgeSet.add(new Edge<>(v, n, NullValue.getInstance()));
            }
        }

        DataSet<Edge<Long, NullValue>> edges = env.fromCollection(edgeSet);
        // create the graph
        return Graph.fromDataSet(edges, env);
    }

    // convert the Gelly result to the expected result
    private WeaklyConnectedComponentsOutput convertResult(DataSet<Tuple2<Long, Long>> result) throws Exception {
        // convert the result to the expected output
        List<Tuple2<Long, Long>> resList = result.collect();
        Map<Long, Long> wccResults = new HashMap<>();
        for (Tuple2<Long, Long> t: resList) {
            wccResults.put(t.f0, t.f1);
        }
        return new WeaklyConnectedComponentsOutput(wccResults);
    }
}
