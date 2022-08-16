from dataclasses import dataclass
import logging
import boto3

AWS_REGION = 'eu-west-1'

@dataclass
class SingleMetric:
    metric_name: str
    dimensions: dict
    value: float
    unit: str = "Count"

    def to_cloudwatch_metric(self):
        return {
            'MetricName': self.metric_name,
            'Dimensions': [{'Name': k, 'Value': v} for k, v in self.dimensions.items()],
            'Unit': self.unit,
            'Value': self.value
        }

class WrapperMetrics:

    def __init__(self, namespace: str, team: str, group: str, aws_region: str = AWS_REGION,client: boto3.client = None) -> None:
        self.namespace = namespace
        self.team=team
        self.group=group
        self.aws_region = aws_region
        self.client = client
        self._metrics = []

    def add(self, *args) -> None:
        self._metrics.extend(args)

    def _iterate(self):
        for m in self._metrics:
            logging.debug(m.to_cloudwatch_metric())
            yield m

    def send(self):
        if not self.namespace or not self.team or not self.group:
            return
        if not self.client:
            self.client = boto3.client('cloudwatch', region_name=self.aws_region)
        logging.debug("sending metrics to cloudwatch {0} namespace with region {1}".format(self.namespace, self.aws_region))
        resp = self.client.put_metric_data(
            MetricData=[m.to_cloudwatch_metric() for m in self._iterate()],
            Namespace=self.namespace
        )
        self._metrics = []
        logging.debug(resp)
        return resp
