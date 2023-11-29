"""
scheduling parameters

resolution
fps
encoding
frame_num
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

        self.ip_dict = {'cloud': '114.212.81.11', 'edge': '192.168.1.2'}

        self.address_dict = {}
        for ip in self.ip_dict:
            self.address_dict[ip] = get_merge_address(self.ip_dict[ip], port=controller_port, path='submit_task')

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
        plan = self.schedule_table[source_id]['plan']
        # del self.schedule_table[source_id]['plan']
        return plan

    def update_scheduler_scenario(self, source_id, scenario_data):
        if source_id not in self.schedule_table:
            raise Exception(f'illegal source id of {source_id}')
        self.schedule_table[source_id]['scenario'] = scenario_data

    def get_cold_start_plan(self, info):
        pass

    def run(self):
        while True:
            for task_schedule in self.schedule_table:
                if 'scenario' not in task_schedule:
                    continue

                scenario = task_schedule['scenario']
                del task_schedule['scenario']

                pid = task_schedule['pid']
                pipeline = scenario['pipeline']
                latency = self.calculate_latency(pipeline)
                pid_out = pid.update(latency)

                meta_data = scenario['meta_data']


            time.sleep(1)

    def adjust_configuration(self, pid_out, meta_data, pipeline):
        pass

    def calculate_latency(self, pipeline):
        latency = 0
        for task in pipeline:
            latency += task['transmit_time']
            if task['service_name'] != 'end':
                latency += task['service_time']
        return latency
