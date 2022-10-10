package lol.schroeder;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.sql.Timestamp;
import java.time.Instant;


@RequiredArgsConstructor
public class DataStreamJob {

	private final SourceFunction<ScaleWeightEvent> source;
	private final SinkFunction<ScaleWeightEvent> sink;

	@SneakyThrows
	public JobExecutionResult execute() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.addSource(source).addSink(sink);

		return env.execute("Scale Weight Event Archival");
	}

	public static void main(String[] args) {
		ParameterTool parameters = ParameterTool.fromArgs(args);
		new DataStreamJob(buildRabbitSource(parameters), buildJdbcSink(parameters))
				.execute();
	}

	public static SinkFunction<ScaleWeightEvent> buildJdbcSink(ParameterTool parameters) {
		String jdbcUrl = parameters.getRequired("jdbcUrl");
		String jdbcUsername = parameters.getRequired("jdbcUsername");
		String jdbcPassword = parameters.getRequired("jdbcPassword");

		return JdbcSink.sink(
				"insert into scale_events (device_id, timestamp, sensor, value) values (?, ?, ?, ?)",
				(statement, weightEvent) -> {
					statement.setString(1, weightEvent.getDeviceId());
					statement.setTimestamp(2, Timestamp.from(Instant.ofEpochSecond(weightEvent.getTime())));
					statement.setString(3, weightEvent.getSensor());
					statement.setDouble(4, weightEvent.getValue());
				},
				JdbcExecutionOptions.builder()
						.withBatchSize(1000)
						.withBatchIntervalMs(200)
						.withMaxRetries(5)
						.build(),
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
						.withUrl(jdbcUrl)
						.withDriverName("org.postgresql.Driver")
						.withUsername(jdbcUsername)
						.withPassword(jdbcPassword)
						.build()
		);
	}

	public static RMQSource<ScaleWeightEvent> buildRabbitSource(ParameterTool parameters) {
		String rabbitHost = parameters.getRequired("rabbitHost");
		int rabbitPort = parameters.getInt("rabbitPort", 5672);
		String rabbitUser = parameters.getRequired("rabbitUser");
		String rabbitPassword = parameters.getRequired("rabbitPassword");
		String rabbitVirtualHost = parameters.get("rabbitVirtualHost", "/");
		String queue = parameters.get("queue");

		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
				.setHost(rabbitHost)
				.setPort(rabbitPort)
				.setUserName(rabbitUser)
				.setPassword(rabbitPassword)
				.setVirtualHost(rabbitVirtualHost)
				.build();

		return new RMQSource<>(
				connectionConfig,
				queue,
				new ScaleWeightEventDeserializationSchema()
		);
	}
}
