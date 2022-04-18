workflowName=$1
workflowId=$2
executedDate="`date "+%Y-%m-%d %H:%M:%S"`"
result=$3
techDate=$4
numberOfLines=$5
failedNode=$6
modifiedDate=$7

echo "{ \"workflowName\": \"$workflowName\", \"workflowId\": \"$workflowId\", \"executedDate\": \"$executedDate\", \"result\": \"$result\", \"techDate\": \"$techDate\", \"numberOfLines\": \"$numberOfLines\", \"failedNode\": \"$failedNode\", \"modifiedDate\": \"$modifiedDate\" }" | hadoop fs -appendToFile - /var/logs/workflow_traces