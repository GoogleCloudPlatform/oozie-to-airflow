SET tez.queue.name=${queueName};
USE awb_${env};

INSERT INTO TABLE traces
SELECT "${workflowName}", "${id}", current_timestamp(), "${result}",
    from_unixtime(unix_timestamp(TRIM("${dateTech}"), 'yyyy-MM-dd HH.mm.ss')),
    "${numberOfLines}", "${failedNode}",
    from_unixtime(unix_timestamp(TRIM("${modifiedDate}"), 'yyyy-MM-dd HH:mm:ss'))
FROM traces
LIMIT 1;