package gelly.graphalytics;

import nl.tudelft.graphalytics.flink.algorithms.lcc.LocalClusteringCoefficient;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.algorithms.lcc.LocalClusteringCoefficientOutput;
import nl.tudelft.graphalytics.validation.algorithms.lcc.LocalClusteringCoefficientValidationTest;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

import java.util.*;

public class LocalClusteringCoefficientJobTest extends LocalClusteringCoefficientValidationTest {

    @Override
    public LocalClusteringCoefficientOutput executeDirectedLocalClusteringCoefficient(
            GraphStructure graphStructure) throws Exception {

        Graph<Long, NullValue, NullValue> input = getInputGraph(graphStructure);
        // run the LCC job
        DataSet<Tuple2<Long, Double>> result = input.run(new LocalClusteringCoefficient());
        return convertResult(result);
    }

    @Override
    public LocalClusteringCoefficientOutput executeUndirectedLocalClusteringCoefficient(
            GraphStructure graphStructure) throws Exception {

        Graph<Long, NullValue, NullValue> input = getInputGraph(graphStructure);
        // run the LCC job
        DataSet<Tuple2<Long, Double>> result = input.run(new LocalClusteringCoefficient());
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
            neighbors.clear();
        }

        DataSet<Edge<Long, NullValue>> edges = env.fromCollection(edgeSet);
        // create the graph
        return Graph.fromDataSet(edges, env);
    }

    // convert the Gelly result to the expected result
    private LocalClusteringCoefficientOutput convertResult(DataSet<Tuple2<Long, Double>> result) throws Exception {
        // convert the result to the expected output
        List<Tuple2<Long, Double>> resList = result.collect();
        Map<Long, Double> lccResults = new HashMap<>();
        for (Tuple2<Long, Double> t: resList) {
            lccResults.put(t.f0, t.f1);
        }
        return new LocalClusteringCoefficientOutput(lccResults);
    }

}
