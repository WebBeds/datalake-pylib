from ._commands import parse_command
from dataclasses import dataclass
from .output import Output

import awswrangler as wr
import pandas as pd
import requests
import io

@dataclass
class Teams(Output):

    name: str
    dimensions: dict
    webhook: str
    condition: list
    
    title: str
    description: str = None

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

        not_meet: bool = False
        for _, row in df.iterrows():

            value = row[self.name.lower()]

            to_eval = [
                str(value),
                *self.condition
            ]

            try:
                if eval(" ".join(to_eval)):
                    print("Condition met, skipping")
                    continue 
            except:
                print("Error evaluating condition")
                continue

            not_meet = True
        
        if not not_meet:
            return

        # DF to Markdown and store in string variable
        buff = io.StringIO()
        df.to_html(buf=buff, index=False)
        buff.seek(0)

        # Using HTML
        text: str = "<h2>Condition not met</h2><p>{}</p>{}<br/><h2>DataFrame</h2>{}".format(
            " ".join(to_eval),
            f"<h2>Description:</h2><p>{self.description}</p>" if self.description else "",
            buff.read()
        )

        # Send POST request to webhook
        message: dict = {
            "@context": "https://schema.org/extensions",
            "@type": "MessageCard",
            "themeColor": "d63333",
            "summary": f"Alarm - There is an issue with {self.title}",
            "title": f"Alarm - There is an issue with {self.title}",
            "text": text
        }

        requests.post(
            url=self.webhook,
            json=message
        )

    @staticmethod
    def parse(data: dict):
        
        webhook: str = parse_command(data.get("options", {}).get("webhook", None), data.get("src", {}))

        name: str = data.get("src", {}).get("name", None)
        if not name:
            raise ValueError("Name must be provided")

        dimensions = data.get("src", {}).get("dimensions", None)
        if not dimensions and data.get("options", {}).get("dimensions", None):
            dimensions = data.get("options", {}).get("dimensions", None)
        if not dimensions:
            raise ValueError("CloudWatch output must have dimensions and unit")

        condition: list = data.get("options", {}).get("condition", None)
        if not condition and not isinstance(condition, list):
            raise ValueError("Condition must be a list")

        title: str = data.get("options", {}).get("title", None)
        if not title:
            raise ValueError("Title must be provided")
        
        description: str = data.get("options", {}).get("description", None)

        return Teams(
            name=name,
            webhook=webhook,
            dimensions=dimensions,
            condition=condition,
            title=title,
            description=description
        )