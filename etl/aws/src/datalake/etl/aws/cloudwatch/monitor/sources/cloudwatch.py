import dataclasses
import datetime
import logging
import typing

import boto3
import pandas as pd

from .source import Source


@dataclasses.dataclass
class CloudWatch(Source):
    namespace: str
    offset: int
    statistic: str
    include: typing.List[dict]
    exclude: typing.List[dict]

    name: str
    dimensions: dict
    unit: str

    _boto3_session: boto3.Session = dataclasses.field(
        init=False, default=boto3.Session(), repr=False
    )

    def get_dimensions(self) -> typing.List[dict]:
        svc = self._boto3_session.client("cloudwatch")
        if all(self.dimensions.values()):
            return [self.dimensions]
        try:
            r = svc.list_metrics(
                Namespace=self.namespace,
                MetricName=self.name,
                Dimensions=[
                    {
                        "Name": k,
                        "Value": v,
                    }
                    for k, v in self.dimensions.items()
                    if v
                ],
            )
            if not r.get("Metrics"):
                raise Exception("No metrics found")
            metrics = r.get("Metrics", [])
            return [
                {
                    k: d.get("Value")
                    for k in self.dimensions.keys()
                    for d in metric.get("Dimensions", [])
                    if k == d.get("Name")
                }
                for metric in metrics
            ]
        except:
            return [self.dimensions]

    def filter_dimensions(self, dims: typing.List[dict]) -> typing.List[dict]:
        if not self.include and not self.exclude:
            return dims
        if self.exclude:
            dims = [
                dim
                for dim in dims
                for excluded in self.exclude
                if any([dim.get(k) != v for k, v in excluded.items()])
            ]
        if self.include:
            dims = [
                dim
                for dim in dims
                for included in self.include
                if any([dim.get(k) == v for k, v in included.items()])
            ]
        return dims

    def retrieve(self, query: str) -> pd.DataFrame:
        out = []
        dims = self.filter_dimensions(self.get_dimensions())
        svc = self._boto3_session.client("cloudwatch")
        for metric in dims:
            try:
                resp = svc.get_metric_statistics(
                    Namespace=self.namespace,
                    MetricName=self.name,
                    Dimensions=[
                        {
                            "Name": k,
                            "Value": v,
                        }
                        for k, v in metric.items()
                    ],
                    StartTime=datetime.datetime.utcnow() - datetime.timedelta(minutes=self.offset),
                    EndTime=datetime.datetime.utcnow(),
                    Period=datetime.timedelta(minutes=self.offset).seconds,
                    Statistics=[
                        self.statistic,
                    ],
                    Unit=self.unit,
                )
                m = {k: v for k, v in metric.items()}
                if not resp.get("Datapoints"):
                    m[self.name] = 0
                else:
                    m[self.name] = resp.get("Datapoints")[0].get(self.statistic)
                out.append(m)
            except:
                logging.warning(f"Failed to retrieve metric {metric}")
                continue
        return pd.DataFrame(out)

    def parse(data: dict) -> "CloudWatch":
        offset = 60
        statistic = "Sum"
        include = []
        exclude = []

        options: dict = data.get("options", {})
        ori_src: dict = data.get("src", {})

        namespace = options.get("namespace")
        if not namespace:
            raise Exception("CloudWatch namespace is required")

        name = ori_src.get("name", options.get("metric"))
        dimensions = ori_src.get("dimensions")
        unit = ori_src.get("unit", "Count")

        try:
            include = options.get("include", include)
            exclude = options.get("exclude", exclude)
        except ValueError:
            pass

        try:
            statistic = options.get("statistic", statistic)
            offset = int(options.get("offset", offset))
        except ValueError:
            pass

        return CloudWatch(
            namespace=namespace,
            offset=offset,
            statistic=statistic,
            include=include,
            exclude=exclude,
            name=name,
            dimensions=dimensions,
            unit=unit,
        )
