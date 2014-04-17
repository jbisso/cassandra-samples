using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Cassandra;

namespace VideoDbApplication.policy
{
    class TimeOfDayRetryPolicy : IRetryPolicy
    {
        private DowngradingConsistencyRetryPolicy policy;
	    private int nonRetryStartHour;
	    private int nonRetryEndHour;

	    public TimeOfDayRetryPolicy(int nonRetryStartHour, int nonRetryEndHour)
        {
		    policy = DowngradingConsistencyRetryPolicy.Instance;
		    this.nonRetryStartHour = nonRetryStartHour;
		    this.nonRetryEndHour = nonRetryEndHour;
	    }

	    public RetryDecision onReadTimeout(Query query, ConsistencyLevel cl,
			    int requiredResponses, int receivedResponses,
			    Boolean dataRetrieved, int nbRetry)
        {
            int hour = DateTime.Now.Hour;
		    if (hour >= nonRetryStartHour && hour <= nonRetryEndHour) {
			    return RetryDecision.Rethrow();
		    }
		    return policy.OnReadTimeout(query, cl, requiredResponses,
				    receivedResponses, dataRetrieved, nbRetry);
	    }

	    public RetryDecision onWriteTimeout(Query query, ConsistencyLevel cl,
			    String writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            int hour = DateTime.Now.Hour;
		    if (hour >= nonRetryStartHour && hour <= nonRetryEndHour) {
			    return RetryDecision.Rethrow();
		    }
		    return policy.OnWriteTimeout(query, cl, writeType, requiredAcks,
				    receivedAcks, nbRetry);
	    }

	    public RetryDecision onUnavailable(Query query, ConsistencyLevel cl,
			    int requiredReplica, int aliveReplica, int nbRetry)
        {
            int hour = DateTime.Now.Hour;
		    if (hour >= nonRetryStartHour && hour <= nonRetryEndHour) {
			    return RetryDecision.Rethrow();
		    }
		    return policy.OnUnavailable(query, cl, requiredReplica, aliveReplica,
				    nbRetry);
	    }
    }
}
