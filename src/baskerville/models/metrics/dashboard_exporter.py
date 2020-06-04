from grafanalib._gen import write_dashboard
from grafanalib.core import Row, SingleStat, Target, Dashboard, Gauge, Graph


class DashboardExporter(object):
    """
    Uses https://github.com/weaveworks/grafanalib to export Metrics from the
    metrics registry
    """

    def __init__(self, dash_title, ds='Prometheus'):
        self.dash_title = dash_title
        self.ds = ds
        self.dashboard = None
        self.rows = []

    def export(self, output_file=''):
        from baskerville.models.metrics.registry import metrics_registry
        panels = []
        for i, (metric_name, value) in enumerate(
                metrics_registry.registry.items()
        ):
            if i % 4 == 0 or i == len(metrics_registry.registry):
                self.rows.append(Row(panels=panels))
                panels = []

            if 'timer' in metric_name:
                g = Gauge()
                g.maxValue = 0
                g.maxValue = 100
                g.show = True
                g.thresholdMarkers = True
                panels.append(
                    SingleStat(
                        title=metric_name,
                        dataSource=self.ds,
                        gauge=g,
                        targets=[
                            Target(
                                expr=f'({metric_name}_sum / {metric_name}_count)',
                                target=metric_name,
                                refId='A',
                                metric=metric_name,
                                datasource=self.ds,
                            )
                        ]
                    )
                )
            else:
                panels.append(
                    Graph(
                        title=metric_name,
                        dataSource=self.ds,
                        targets=[
                            Target(
                                expr=f'{metric_name}_total'
                                if 'total' in metric_name else metric_name,
                                target=metric_name,
                                refId='A',
                                metric=metric_name,
                                datasource=self.ds
                            )
                        ]
                    )

                )

        for panel in panels:
            self.rows.append(Row(panels=[panel]))

        self.dashboard = Dashboard(
            title=self.dash_title,
            rows=self.rows
        ).auto_panel_ids()

        with open(output_file, 'w') as f:
            write_dashboard(self.dashboard, f)
