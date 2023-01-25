from ....ecs.wrapper.metrics import SingleMetric
from ..metrics import MonitorMetrics
from dataclasses import dataclass
from .output import Output

import pandas as pd
import logging

DEFAULT_UNITS = "Seconds"

@dataclass
class CloudWatch(Output):

    name: str
    dimensions: dict
    unit: str

    namespace: str
    region: str

    def _process_dimensions(
        self,
        dimension,
        default,
        input
    ):
        if str(dimension).lower() in input:
            return str(input[str(dimension).lower()])
        else:
            return default

    def send(self, df: pd.DataFrame) -> None:

        metrics = MonitorMetrics(
            namespace=self.namespace,
            aws_region=self.region,
        )

        for _, row in df.iterrows():
            m = SingleMetric(
                metric_name=self.name,
                dimensions={
                    k: self._process_dimensions(
                        k,
                        v,
                        row
                    ) for k, v in self.dimensions.items()
                },
                value=row[self.name.lower()],
                unit=self.unit
            )
            logging.info(m.to_cloudwatch_metric())
            metrics.add(m)

        metrics.send()

    @staticmethod
    def parse(data: dict):

        name: str = None
        dimensions: dict = None
        unit: str = None

        original_src: dict = data.get("src", None)
        if not original_src:
            raise ValueError("Source must have src")
        
        # Check original source
        name = original_src.get("name", None)
        dimensions = original_src.get("dimensions", None)
        unit: str = original_src.get("unit", None)
        namespace: str = original_src.get("namespace", None)

        if not dimensions and data.get("options", {}).get("dimensions", None):
            dimensions = data.get("options", {}).get("dimensions", None)
        if not unit and data.get("options", {}).get("unit", None):
            unit = data.get("options", {}).get("unit", None)
        if not namespace and data.get("options", {}).get("namespace", None):
            namespace: str = data.get("options", {}).get("namespace", None)

        if not dimensions:
            raise ValueError("CloudWatch output must have dimensions and unit")
        if not unit:
            unit = DEFAULT_UNITS

        region: str = data.get("options", {}).get("region", "eu-west-1")
        
        return CloudWatch(
            name=name,
            dimensions=dimensions,
            unit=unit,
            namespace=namespace,
            region=region,
        )