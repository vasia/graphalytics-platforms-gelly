package gelly.graphalytics;

import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import nl.tudelft.graphalytics.flink.algorithms.cdlp.LabelPropagation;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.algorithms.cdlp.CommunityDetectionLPOutput;
import nl.tudelft.graphalytics.validation.algorithms.cdlp.CommunityDetectionLPValidationTest;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

import java.util.*;

public class LabelPropagationJobTest extends CommunityDetectionLPValidationTest {

    @Override
    public CommunityDetectionLPOutput executeDirectedCommunityDetection(
            GraphStructure graphStructure, CommunityDetectionLPParameters params) throws Exception {

        Graph<Long, NullValue, NullValue> input = getInputGraph(graphStructure, false);
        // run the Label Propagation job
        DataSet<Tuple2<Long, Long>> result = input.run(new LabelPropagation(params, true));
        return convertResult(result);
    }

    @Override
    public CommunityDetectionLPOutput executeUndirectedCommunityDetection(
            GraphStructure graphStructure, CommunityDetectionLPParameters params) throws Exception {

        Graph<Long, NullValue, NullValue> input = getInputGraph(graphStructure, true);
        // run the Label Propagation job
        DataSet<Tuple2<Long, Long>> result = input.run(new LabelPropagation(params, false));
        return convertResult(result);
    }

    // helper method to create the input Gelly Graph from the GraphStructure
    private Graph<Long, NullValue, NullValue> getInputGraph(GraphStructure graphStructure, boolean undirected) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // get the vertices
        Set<Long> vertexSet = graphStructure.getVertices();

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
        return Graph.fromDataSet(edges, env);
    }

    // convert the Gelly result to the expected result
    private CommunityDetectionLPOutput convertResult(DataSet<Tuple2<Long, Long>> result) throws Exception {
        // convert the result to the expected output
        List<Tuple2<Long, Long>> resList = result.collect();
        Map<Long, Long> lpResults = new HashMap<>();
        for (Tuple2<Long, Long> t: resList) {
            lpResults.put(t.f0, t.f1);
        }
        return new CommunityDetectionLPOutput(lpResults);
    }

}
