package gelly.graphalytics;

import science.atlarge.graphalytics.domain.algorithms.PageRankParameters;
import science.atlarge.graphalytics.flink.algorithms.pr.ScatterGatherPageRank;
import science.atlarge.graphalytics.validation.GraphStructure;
import science.atlarge.graphalytics.validation.algorithms.pr.PageRankOutput;
import science.atlarge.graphalytics.validation.algorithms.pr.PageRankValidationTest;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

import java.util.*;

public class GatherScatterPageRankJobTest extends PageRankValidationTest {

    @Override
    public PageRankOutput executeDirectedPageRank(
            GraphStructure graphStructure, PageRankParameters params) throws Exception {

        Graph<Long, NullValue, NullValue> input = getInputGraph(graphStructure);
        // run the PageRank job
        DataSet<Tuple2<Long, Double>> result = input.run(
                new ScatterGatherPageRank(params, graphStructure.getVertices().size()));
        return convertResult(result);
    }

    @Override
    public PageRankOutput executeUndirectedPageRank(
            GraphStructure graphStructure, PageRankParameters params) throws Exception {

        Graph<Long, NullValue, NullValue> input = getInputGraph(graphStructure);
        // run the PageRank job
        DataSet<Tuple2<Long, Double>> result = input.run(
                new ScatterGatherPageRank(params, graphStructure.getVertices().size()));
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
    private PageRankOutput convertResult(DataSet<Tuple2<Long, Double>> result) throws Exception {
        // convert the result to the expected output
        List<Tuple2<Long, Double>> resList = result.collect();
        Map<Long, Double> prResults = new HashMap<>();
        for (Tuple2<Long, Double> t: resList) {
            prResults.put(t.f0, t.f1);
        }
        return new PageRankOutput(prResults);
    }
}
