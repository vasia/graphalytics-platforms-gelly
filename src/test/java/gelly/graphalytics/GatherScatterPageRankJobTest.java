package gelly.graphalytics;

import nl.tudelft.graphalytics.domain.algorithms.PageRankParameters;
import nl.tudelft.graphalytics.flink.algorithms.pr.ScatterGatherPageRank;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.algorithms.pr.PageRankOutput;
import nl.tudelft.graphalytics.validation.algorithms.pr.PageRankValidationTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

import java.util.*;

public class GatherScatterPageRankJobTest extends PageRankValidationTest {

    @Override
    public PageRankOutput executeDirectedPageRank(
            GraphStructure graphStructure, PageRankParameters params) throws Exception {

        Graph<Long, Double, Double> input = getInputGraph(graphStructure);
        // run the PageRank job
        DataSet<Vertex<Long, Double>> result = input.run(
                new ScatterGatherPageRank<Long>(params, graphStructure.getVertices().size()));
        return convertResult(result);
    }

    @Override
    public PageRankOutput executeUndirectedPageRank(
            GraphStructure graphStructure, PageRankParameters params) throws Exception {

        Graph<Long, Double, Double> input = getInputGraph(graphStructure);
        // run the PageRank job
        DataSet<Vertex<Long, Double>> result = input.run(
                new ScatterGatherPageRank<Long>(params, graphStructure.getVertices().size()));
        return convertResult(result);
    }

    // helper method to create the input Gelly Graph from the GraphStructure
    private Graph<Long, Double, Double> getInputGraph(GraphStructure graphStructure) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // get the vertices
        Set<Long> vertexSet = graphStructure.getVertices();

        DataSet<Vertex<Long, Double>> vertices = env.fromCollection(vertexSet)
                .map(new MapFunction<Long, Vertex<Long, Double>>() {
                    @Override
                    public Vertex<Long, Double> map(Long id) {
                        return new Vertex<>(id, 1.0);
                    }
                });

        // get the edges
        Set<Edge<Long, Double>> edgeSet = new HashSet<>();
        for (Long v: vertexSet) {
            Set<Long> neighbors = graphStructure.getEdgesForVertex(v);
            for (Long n: neighbors) {
                edgeSet.add(new Edge<>(v, n, 1.0));
            }
        }

        DataSet<Edge<Long, Double>> edges = env.fromCollection(edgeSet);
        // create the graph
        return Graph.fromDataSet(vertices, edges, env);
    }

    // convert the Gelly result to the expected result
    private PageRankOutput convertResult(DataSet<Vertex<Long, Double>> result) throws Exception {
        // convert the result to the expected output
        List<Vertex<Long, Double>> resList = result.collect();
        Map<Long, Double> prResults = new HashMap<>();
        for (Vertex<Long, Double> t: resList) {
            prResults.put(t.getId(), t.getValue());
        }
        return new PageRankOutput(prResults);
    }
}
