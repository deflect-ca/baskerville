from baskerville.models.base import BaskervilleBase
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_factory import PipelineFactory
from baskerville.util.helpers import get_logger
from baskerville.models.metrics.registry import metrics_registry


class BaskervilleAnalyticsEngine(BaskervilleBase):
    """
    The Baskerville Analytics Engine's main.
    Sets up the pipeline and runs it.
    """

    def __init__(self, run_type, conf, register_metrics=True):
        super(BaskervilleAnalyticsEngine, self).__init__(conf)
        self.run_type = run_type
        self.pipeline = None
        self.performance_stats = None

        # set config's logger
        BaskervilleConfig.set_logger(
            conf['engine']['log_level'],
            conf['engine']['logpath']
        )
        self.config = BaskervilleConfig(self.config).validate()

        self.register_metrics = (
            self.config.engine.metrics and register_metrics
        )

        self.logger = get_logger(
            self.__class__.__name__,
            logging_level=conf['engine']['log_level'],
            output_file=conf['engine']['logpath']
        )

    def _set_up_pipeline(self):
        self.logger.debug('Setting up the pipeline')
        return PipelineFactory().get_pipeline(self.run_type, self.config)

    def _register_metrics(self):
        if self.register_metrics:
            if self.config.engine.metrics.performance:
                self.performance_stats = self._register_performance_stats()
            # register progress metrics
            if self.config.engine.metrics.progress:
                self.register_pipeline_metrics()

    def _register_performance_stats(self):
        """
        Registers the performance metrics configured:
        Pipeline metrics: Which methods to time
        Cache metrics: Which methods to cache
        Feature metrics: Time the compute / update methods of features
        :return: an instance of PerformanceStatsRegistry
        :rtype: PerformanceStatsRegistry
        """

        # time pipeline methods
        if self.config.engine.metrics.performance.get('pipeline'):
            method_to_action = {
                v.__name__: k
                for k, v
                in self.pipeline.step_to_action.items()
            }
            for f_name in self.config.engine.metrics.performance['pipeline']:
                if hasattr(self.pipeline, f_name):
                    wrapper_f = metrics_registry.register_timer(
                        f'timer_for_pipeline_{f_name}',
                        getattr(self.pipeline, f_name)
                    )
                    setattr(
                        self.pipeline,
                        f_name,
                        wrapper_f
                    )

                    step_name = method_to_action.get(wrapper_f.__name__, None)
                    if step_name:
                        self.pipeline.step_to_action[step_name] = wrapper_f

        # time cache
        if self.pipeline.request_set_cache and \
                self.config.engine.metrics.performance.get('request_set_cache'):
            for f_name in self.config.engine.metrics.performance[
                'request_set_cache'
            ]:
                if hasattr(self.pipeline.request_set_cache, f_name):
                    wrapper_f = metrics_registry.register_timer(
                        f'timer_for_request_set_cache_{f_name}',
                        getattr(self.pipeline.request_set_cache, f_name)
                    )
                    setattr(
                        self.pipeline.request_set_cache,
                        f_name,
                        wrapper_f
                    )

        # time feature computation
        if self.config.engine.metrics.performance.get('features'):
            # todo: time the update method too
            if 'feature_manager' in self.pipeline.__dict__:
                for feature in self.pipeline.feature_manager.active_features:
                    wrapper_f = metrics_registry.register_timer(
                        f'timer_for_feature_{feature.feature_name}',
                        getattr(feature, 'compute')
                    )
                    setattr(
                        feature,
                        'compute',
                        wrapper_f
                    )

        return metrics_registry

    def register_pipeline_metrics(self):
        """
        Registers action hooks for:
        - step_to_action: enum metric to show the progress
        - get_data:
            - 'total_number_of_requests': increments the metric counter by
            the self.logs_df.count()
            - 'current_number_of_requests': sets the metric counter to the
            current self.logs_df.count()
        - filter_columns:
            - `total number of requests after filtering`
        - group_by:
            - `current_number_of_request_sets`:increments the metric counter by
            the self.logs_df.count()

        - save_df_to_table: average host prediction

        :return: None
        """
        from baskerville.models.metrics.registry import metrics_registry
        from baskerville.util.enums import MetricClassEnum
        from baskerville.models.metrics.helpers import (
            incr_counter_for_logs_df,
            update_avg_hosts_counter,
            set_counter_for_logs_df,
            set_accumulators
        )
        set_accumulators(self.pipeline.spark)

        # wrap step_to_action
        self.pipeline.step_to_action = metrics_registry.register_states(
            'baskerville_steps',
            'Tracks which step Baskerville is currently executing',
            self.pipeline.step_to_action
        )

        # total number of requests in current df
        get_data = metrics_registry.register_action_hook(
            self.pipeline.get_data,
            incr_counter_for_logs_df,
            metric_name='total_number_of_requests'
        )
        # current number of requests in current df
        get_data = metrics_registry.register_action_hook(
            get_data,
            set_counter_for_logs_df,
            metric_cls=MetricClassEnum.gauge,
            metric_name='current_number_of_requests'
        )
        # current number of requests in current df
        filter_columns = metrics_registry.register_action_hook(
            self.pipeline.filter_columns,
            incr_counter_for_logs_df,
            metric_cls=MetricClassEnum.counter,
            metric_name='total_number_of_requests_after_filtering'
        )
        # number of request_sets in current df after group by
        group_by = metrics_registry.register_action_hook(
            self.pipeline.group_by,
            incr_counter_for_logs_df,
            metric_name='current_number_of_request_sets'
        )

        # set the average prediction for each host, per batch
        save_df_to_table = metrics_registry.register_action_hook(
            self.pipeline.save_df_to_table,
            update_avg_hosts_counter,
            metric_cls=MetricClassEnum.gauge,
            metric_name='avg_host_prediction',
            labelnames=['target']
        )

        # set wrapped methods
        setattr(self.pipeline, 'get_data', get_data)
        setattr(self.pipeline, 'filter_columns', filter_columns)
        setattr(self.pipeline, 'group_by', group_by)
        setattr(self.pipeline, 'save_df_to_table', save_df_to_table)

        if self.config.engine.metrics.exported_dashboard_file:
            from baskerville.models.metrics.dashboard_exporter import \
                DashboardExporter
            dashboard = DashboardExporter('Baskerville Metrics')
            dashboard.export(
                self.config.engine.metrics.exported_dashboard_file)

        self.logger.info('Registered metrics.')

    def run(self) -> None:
        """
        Run steps:
        - Set up a pipeline,
        - initialize it,
        - register metrics (if configured)
        - run the pipeline
        :return: None
        """
        self.pipeline = self._set_up_pipeline()
        self.pipeline.initialize()
        self._register_metrics()
        self.pipeline.run()

    def finish_up(self):
        import sys
        self.logger.info(
            'Exiting: please, hold while {} pipeline finishes up...'.format(
                self.pipeline.__class__.__name__
            )
        )
        if self.pipeline:
            self.pipeline.finish_up()
        self.logger.info('{} says \'Goodbye\'.'.format(
            self.__class__.__name__
        )
        )
        sys.exit(0)
