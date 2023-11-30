"""
scheduling parameters

resolution
fps
encoding
pipeline[{execute_address}, {execute_address}]
priority
"""
import time

from pid import PIDController
from utils import *

user_constraint = 0.3

cloud_ip = '114.212.81.11'
edge_ip = '192.168.1.2'

controller_port = 9002


class Scheduler:
    def __init__(self):
        self.schedule_table = {}

        self.schedule_interval = 1

        self.ip_dict = {'cloud': '114.212.81.11', 'edge': '192.168.1.2'}

        self.address_dict = {}
        for ip in self.ip_dict:
            self.address_dict[ip] = get_merge_address(self.ip_dict[ip], port=controller_port, path='submit_task')

        self.address_diverse_dict = {v: k for k, v in self.address_dict.items()}

    def register_schedule_table(self, source_id):
        if source_id in self.schedule_table:
            return
        self.schedule_table[source_id] = {}
        pid = PIDController()
        pid.set_setpoint(user_constraint)
        self.schedule_table[source_id]['pid'] = pid

    def get_schedule_plan(self, info):
        source_id = info['source_id']
        if 'plan' not in self.schedule_table[source_id]:
            return self.get_cold_start_plan(info)
        return self.schedule_table[source_id]['plan']

    def update_scheduler_scenario(self, source_id, scenario_data):
        if source_id not in self.schedule_table:
            raise Exception(f'illegal source id of {source_id}')
        self.schedule_table[source_id]['scenario'] = scenario_data

    def get_cold_start_plan(self, info):
        plan = {
            'resolution': '720p',
            'fps': 20,
            'encoding': 'mp4v',
            'priority': 0,
            'pipeline': info['pipeline']
        }
        return plan

    def run(self):
        while True:
            for task_schedule in self.schedule_table:
                if 'scenario' not in task_schedule:
                    continue

                scenario = task_schedule['scenario']
                del task_schedule['scenario']

                pid = task_schedule['pid']
                pipeline = scenario['pipeline']
                meta_data = scenario['meta_data']

                latency = self.calculate_latency(pipeline)
                pid_out = pid.update(latency)

                plan = self.adjust_configuration(pid_out, meta_data, pipeline)
                task_schedule['plan'] = plan

            # schedule interval
            time.sleep(self.schedule_interval)

    def adjust_configuration(self, pid_out, meta_data, pipeline):
        position = self.map_pipeline_2_position(pipeline)

        return {
            'resolution': None,
            'fps': None,
            'encoding': 'mp4v',
            'priority': 0,
            'pipeline': self.map_position_2_pipeline(position, pipeline)
        }

    def calculate_latency(self, pipeline):
        latency = 0
        for task in pipeline:
            latency += task['transmit_time']
            if task['service_name'] != 'end':
                latency += task['service_time']
        return latency

    def map_pipeline_2_position(self, pipeline):
        position = []
        for task in pipeline:
            if task['service_name'] == 'end':
                break

            position.append(self.address_diverse_dict[task['execute_address']])

        return position

    def map_position_2_pipeline(self, position, pipeline):
        assert len(position) + 1 == len(pipeline)
        for i, pos in enumerate(position):
            pipeline[i]['execute_address'] = self.address_dict[pos]

        return pipeline
