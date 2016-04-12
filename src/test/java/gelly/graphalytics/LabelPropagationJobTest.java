package gelly.graphalytics;

import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import nl.tudelft.graphalytics.flink.algorithms.cdlp.LabelPropagation;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.algorithms.cdlp.CommunityDetectionLPOutput;
import nl.tudelft.graphalytics.validation.algorithms.cdlp.CommunityDetectionLPValidationTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.util.*;

public class LabelPropagationJobTest extends CommunityDetectionLPValidationTest {

    @Override
    public CommunityDetectionLPOutput executeDirectedCommunityDetection(
            GraphStructure graphStructure, CommunityDetectionLPParameters params) throws Exception {

        Graph<Long, Long, NullValue> input = getInputGraph(graphStructure, false);
        // run the Label Propagation job
        DataSet<Vertex<Long, Long>> result = input.run(new LabelPropagation(params, true));
        return convertResult(result);
    }

    @Override
    public CommunityDetectionLPOutput executeUndirectedCommunityDetection(
            GraphStructure graphStructure, CommunityDetectionLPParameters params) throws Exception {

        Graph<Long, Long, NullValue> input = getInputGraph(graphStructure, true);
        // run the Label Propagation job
        DataSet<Vertex<Long, Long>> result = input.run(new LabelPropagation(params, false));
        return convertResult(result);
    }

    // helper method to create the input Gelly Graph from the GraphStructure
    private Graph<Long, Long, NullValue> getInputGraph(GraphStructure graphStructure, boolean undirected) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // get the vertices
        Set<Long> vertexSet = graphStructure.getVertices();

        DataSet<Vertex<Long, Long>> vertices = env.fromCollection(vertexSet)
                .map(new MapFunction<Long, Vertex<Long, Long>>() {
                    @Override
                    public Vertex<Long, Long> map(Long id) {
                        return new Vertex<>(id, id);
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
    private CommunityDetectionLPOutput convertResult(DataSet<Vertex<Long, Long>> result) throws Exception {
        // convert the result to the expected output
        List<Vertex<Long, Long>> resList = result.collect();
        Map<Long, Long> lpResults = new HashMap<>();
        for (Vertex<Long, Long> t: resList) {
            lpResults.put(t.getId(), t.getValue());
        }
        return new CommunityDetectionLPOutput(lpResults);
    }

}
