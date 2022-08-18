import logging
import boto3

AWS_REGION = "eu-west-1"

class MonitorMetrics:

    def __init__(self, namespace: str, aws_region: str = AWS_REGION,client: boto3.client = None) -> None:
        self.namespace = namespace
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
        if not self.namespace:
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