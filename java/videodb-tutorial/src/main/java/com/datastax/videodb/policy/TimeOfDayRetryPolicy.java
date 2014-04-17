package com.datastax.videodb.policy;

import java.util.Calendar;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

public class TimeOfDayRetryPolicy implements RetryPolicy {

	private final DowngradingConsistencyRetryPolicy policy;
	private final int nonRetryStartHour;
	private final int nonRetryEndHour;

	public TimeOfDayRetryPolicy(int nonRetryStartHour, int nonRetryEndHour) {
		policy = DowngradingConsistencyRetryPolicy.INSTANCE;

		this.nonRetryStartHour = nonRetryStartHour;
		this.nonRetryEndHour = nonRetryEndHour;
	}

	@Override
	public RetryDecision onReadTimeout(Query query, ConsistencyLevel cl,
			int requiredResponses, int receivedResponses,
			boolean dataRetrieved, int nbRetry) {

		int hour = Calendar.getInstance().get(Calendar.HOUR);

		if (hour >= nonRetryStartHour && hour <= nonRetryEndHour) {

			return RetryDecision.rethrow();
		}

		return policy.onReadTimeout(query, cl, requiredResponses,
				receivedResponses, dataRetrieved, nbRetry);
	}

	@Override
	public RetryDecision onWriteTimeout(Query query, ConsistencyLevel cl,
			WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
		int hour = Calendar.getInstance().get(Calendar.HOUR);

		if (hour >= nonRetryStartHour && hour <= nonRetryEndHour) {

			return RetryDecision.rethrow();
		}

		return policy.onWriteTimeout(query, cl, writeType, requiredAcks,
				receivedAcks, nbRetry);
	}

	@Override
	public RetryDecision onUnavailable(Query query, ConsistencyLevel cl,
			int requiredReplica, int aliveReplica, int nbRetry) {
		int hour = Calendar.getInstance().get(Calendar.HOUR);

		if (hour >= nonRetryStartHour && hour <= nonRetryEndHour) {

			return RetryDecision.rethrow();
		}

		return policy.onUnavailable(query, cl, requiredReplica, aliveReplica,
				nbRetry);
	}

}
