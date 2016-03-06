package nl.tudelft.graphalytics.flink.algorithms.bfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.graph.utils.VertexToTuple2Map;
import org.apache.flink.types.NullValue;

public class ScatterGatherBFS<K> implements GraphAlgorithm<K, Long, NullValue, DataSet<Tuple2<K, Long>>> {

	private final K srcVertexId;
	private final Integer maxIterations;

	/**
	 * @param srcVertexId The ID of the source vertex.
	 * @param maxIterations The maximum number of iterations to run.
	 */
	public ScatterGatherBFS(K srcVertexId, Integer maxIterations) {
		this.srcVertexId = srcVertexId;
		this.maxIterations = maxIterations;
	}

	@Override
	public DataSet<Tuple2<K, Long>> run(Graph<K, Long, NullValue> input) {

		return input.mapVertices(new InitVerticesMapper<K>(srcVertexId))
				.runScatterGatherIteration(new VertexDistanceUpdater<K>(), new MinDistanceMessenger<K>(),
				maxIterations).getVertices().map(new VertexToTuple2Map<K, Long>());
	}

	@SuppressWarnings("serial")
	public static final class InitVerticesMapper<K>	implements MapFunction<Vertex<K, Long>, Long> {

		private K srcVertexId;

		public InitVerticesMapper(K srcId) {
			this.srcVertexId = srcId;
		}

		public Long map(Vertex<K, Long> value) {
			if (value.f0.equals(srcVertexId)) {
				return 0l;
			} else {
				return Long.MAX_VALUE;
			}
		}
	}

	/**
	 * Function that updates the value of a vertex by picking the minimum
	 * distance from all incoming messages.
	 * 
	 * @param <K>
	 */
	@SuppressWarnings("serial")
	public static final class VertexDistanceUpdater<K> extends VertexUpdateFunction<K, Long, Long> {

		@Override
		public void updateVertex(Vertex<K, Long> vertex, MessageIterator<Long> inMessages) {

			long minDistance = Long.MAX_VALUE;

			for (long msg : inMessages) {
				if (msg < minDistance) {
					minDistance = msg;
				}
			}

			if (vertex.getValue() > minDistance) {
				setNewVertexValue(minDistance);
			}
		}
	}

	/**
	 * Distributes the minimum distance associated with a given vertex among all
	 * the target vertices summed up with the edge's value.
	 * 
	 * @param <K>
	 */
	@SuppressWarnings("serial")
	public static final class MinDistanceMessenger<K> extends MessagingFunction<K, Long, Long, NullValue> {

		@Override
		public void sendMessages(Vertex<K, Long> vertex) {
			if (vertex.getValue() < Long.MAX_VALUE) {
				sendMessageToAllNeighbors(vertex.getValue() + 1);
			}
		}
	}

}
