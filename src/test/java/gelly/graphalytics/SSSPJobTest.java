package gelly.graphalytics;

import nl.tudelft.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters;
import nl.tudelft.graphalytics.flink.algorithms.sssp.ScatterGatherSSSP;
import nl.tudelft.graphalytics.util.graph.PropertyGraph;
import nl.tudelft.graphalytics.validation.algorithms.sssp.SingleSourceShortestPathsOutput;
import nl.tudelft.graphalytics.validation.algorithms.sssp.SingleSourceShortestPathsValidationTest;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

import java.util.*;

public class SSSPJobTest extends SingleSourceShortestPathsValidationTest {

    @Override
    public SingleSourceShortestPathsOutput executeDirectedSingleSourceShortestPaths(
            PropertyGraph<Void, Double> propertyGraph,
            SingleSourceShortestPathsParameters params) throws Exception {

        Graph<Long, Double, Double> input = getInputGraph(propertyGraph);
        // run the SSSP job
        DataSet<Vertex<Long, Double>> result = input.run(new ScatterGatherSSSP(params));
        return convertResult(result);
    }

    @Override
    public SingleSourceShortestPathsOutput executeUndirectedSingleSourceShortestPaths(
            PropertyGraph<Void, Double> propertyGraph,
            SingleSourceShortestPathsParameters params) throws Exception {

        Graph<Long, Double, Double> input = getInputGraph(propertyGraph);
        // run the SSSP job
        DataSet<Vertex<Long, Double>> result = input.run(new ScatterGatherSSSP(params));
        return convertResult(result);
    }

    // helper method to create the input Gelly Graph from the GraphStructure
    private Graph<Long, Double, Double> getInputGraph(PropertyGraph<Void, Double> graph) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // get the vertices and edges
        Collection<PropertyGraph<Void, Double>.Vertex> vertexSet = graph.getVertices();

        Set<Vertex<Long, Double>> flinkVertexSet = new HashSet<>();
        Set<Edge<Long, Double>> edgeSet = new HashSet<>();

        for (PropertyGraph.Vertex v: vertexSet) {
            // create a vertex
            flinkVertexSet.add(new Vertex<>(v.getId(), Double.POSITIVE_INFINITY));
            Collection<PropertyGraph.Edge> neighbors = v.getOutgoingEdges();
            // create its edges
            for (PropertyGraph.Edge e: neighbors) {
                edgeSet.add(new Edge<>(
                        e.getSourceVertex().getId(), e.getDestinationVertex().getId(), (Double)e.getValue()));
            }
        }

        DataSet<Vertex<Long, Double>> vertices = env.fromCollection(flinkVertexSet);
        DataSet<Edge<Long, Double>> edges = env.fromCollection(edgeSet);
        // create the graph
        return Graph.fromDataSet(vertices, edges, env);
    }

    // convert the Gelly result to the expected result
    private SingleSourceShortestPathsOutput convertResult(DataSet<Vertex<Long, Double>> result) throws Exception {
        // convert the result to the expected output
        List<Vertex<Long, Double>> resList = result.collect();
        Map<Long, Double> ssspResults = new HashMap<>();
        for (Vertex<Long, Double> t: resList) {
            ssspResults.put(t.f0, t.f1);
        }
        return new SingleSourceShortestPathsOutput(ssspResults);
    }

}
