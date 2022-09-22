# **ECS Wrapper**

This module is a wrapper around the AWS ECS Tasks. It provides a powerful way to run ECS Tasks and get metrics and functionality around it.

## **Install**

Using **pip**:
```bash
pip3 install "git+https://www.github.com/Webjet/datalake-pylib#egg=datalake-etl-aws&subdirectory=etl/aws"
```

## **Usage**

The wrapper is used as a command where you can pass in the following arguments:

- **config** (`default: ./wrapper.conf`): The path to the config file.
- **cli-json** (`required`): The json string where the arguments for the task are passed in.
- **dry**: If this is set to true, metrics and actions will not be performed.
- **debug**: If this is set to true, debug logs will be printed.

```bash
twrap \
    --config <config_path> \
    --cli-json <cli_json> \
    --dry \
    --debug
```

## **CLI JSON Template**

```python
{
    "job": "<job_name>", # The name of the job used for CloudWatch metrics.
    "entrypoint": "<entrypoint>", # The entrypoint of the task (can be declared as default on the config file)
    "command": [
        "<command/s>"
    ], # The array of commands to be passed to the wrapper and be executed.
    "actions": [
        {
            "stage": "<[end | start]>", # The stage where the action will be executed. (start is before execute command), (end is after execute command)
            "plugin": "<plugin_type>", # The type of plugin to be used (default is `http`)
            "condition": [
                "<context_variable>",
                "<operator>",
                "<value>"
            ], # The condition to be evaluated before executing the action. (array for declare a condition)
            "key": "value" # values to be passed to the plugin requirements.
        }
    ] # The array of actions to be performed.
}
```

## **Action Example**

``Context``: Before execute the command, send an http post requests to an endpoint, then if the task fails send a failed message to an endpoint, or if goes well send a completed message to an endpoint.

```python
{
    "actions": [
        # Running phase
        {
            "stage": "start",
            "plugin": "http",
            "method": "POST",
            "url": "<endpoint>",
            "headers": {
                "Content-Type": "application/json",
                "Authorization": "<auth-token>"
            },
            "params": {
                "status": "running"
            }
        },
        # Failed phase
        {
            "stage": "end",
            "plugin": "http",
            "method": "POST",
            "url": "<endpoint>",
            "headers": {
                "Content-Type": "application/json",
                "Authorization": "<auth-token>"
            },
            "params": {
                "status": "failed"
            },
            "condition": [
                "${{oenv.ExitCode}}", # Use a plugin variable.
                "!=",
                "0"
            ]
        },
        # Completed phase
        {
            "stage": "end",
            "plugin": "http",
            "method": "POST",
            "url": "<endpoint>",
            "headers": {
                "Content-Type": "application/json",
                "Authorization": "<auth-token>"
            },
            "params": {
                "status": "completed"
            },
            "condition": [
                "${{oenv.ExitCode}}", # Use a plugin variable.
                "==",
                "0"
            ]
        },
    ]
}
```

## **OENV Data**

The oenv data are variables to be used on commands, headers, params, etc. These variables comes from StepFunctions data and updated with process data (ExitCode and Duration).

```python
{
    "AWS_EXECUTION_ARN": "<aws_execution_arn>", # ENV Variable (StepFunction usage)
    "AWS_EXECUTION_START": "<aws_execution_start>", # ENV Variable (StepFunction usage)
    "AWS_STATE_MACHINE_ID": "<aws_state_machine_id>", # ENV Variable (StepFunction usage)
    "AWS_STATE_ENTERED": "<aws_state_entered>", # ENV Variable (StepFunction usage)
    "CreationTimestampSeconds": "<creation_timestamp_seconds>", # Parsed Variable (Comes from parsed AWS_EXECUTION_START)
    "CreationTimestampMilliseconds": "<creation_timestamp_milliseconds>", # Parsed Variable (Comes from parsed AWS_EXECUTION_START)
    "ExecutionId": "<execution_id>", # Parsed Variable (Comes from parsed AWS_EXECUTION_ARN)
    "ExitCode": "<exit_code>", # Process Variable (Comes from the process exit code) (Only available on end stage)
    "Duration": "<duration>", # Process Variable (Comes from the process duration) (Only available on end stage)
}
```

## **Parse Plugins**

The parse plugins are used to parse the different variables for the actions or commands, the plugins are:
* **oenv**: This plugin is used for use the oenv variables inside the actions or commands.
* **secretsmanager**: This plugin is used for get secrets from AWS Secrets Manager and be used inside the actions or commands.

Usage of the parsed variables.
```python
{
    "commands": [
        "script.py",
        "--epoch",
        "${{oenv.CreationTimestampSeconds}}", # Use a oenv plugin variable.
    ],
    "actions": [
        {
            "stage": "start",
            "plugin": "http",
            "method": "POST",
            "url": "${{secretsmanager.endpoint}}", # Use a secretsmanager plugin variable.
            "headers": {
                "Content-Type": "application/json",
                "Authorization": {
                    "commands": [
                        "Bearer",
                        "${{secretsmanager.token}}" # Use a secretsmanager plugin variable.
                    ]
                } # ReplacedCommand usage, join different data into a string.
            },
            "params": {
                "executionid": "${{oenv.ExecutionId}}", # Use a oenv plugin variable.
                "status": "running",
                "environment": "${{secretsmanager.environment}}" # Use a secretsmanager plugin variable.
            }
        }
    ]
}
```
