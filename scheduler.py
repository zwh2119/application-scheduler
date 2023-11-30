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

        self.resolution_list = ['360p', '720p', '1080p']
        self.fps_list = [1, 5, 10, 15, 20, 25, 30]

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

                plan = self.adjust_plan_configuration(pid_out, meta_data, pipeline)
                task_schedule['plan'] = plan

            # schedule interval
            time.sleep(self.schedule_interval)

    def adjust_plan_configuration(self, pid_out, meta_data, pipeline):
        position = self.map_pipeline_2_position(pipeline)
        resolution = meta_data['resolution']
        fps = meta_data['fps']
        resolution_raw = meta_data['resolution_raw']
        fps_raw = meta_data['fps_raw']

        done = False
        if pid_out > 0:
            if pid_out > 3:
                position, done = self.change_position(position, 1)
            if pid_out > 2 or not done:
                resolution, done = self.change_single_configuration(self.resolution_list, 1, resolution, resolution_raw)
            if pid_out > 1 or not done:
                fps, done = self.change_single_configuration(self.fps_list, 1, fps, fps_raw)
        if pid_out < 0:
            if pid_out < -3 or not done:
                position, done = self.change_position(position, -1)
            if pid_out < -2 or not done:
                resolution, done = self.change_single_configuration(self.resolution_list, -1, resolution,
                                                                    resolution_raw)
            if pid_out < -1:
                fps, done = self.change_single_configuration(self.fps_list, -1, fps, fps_raw)

        return {
            'resolution': resolution,
            'fps': fps,
            'encoding': 'mp4v',
            'priority': 0,
            'pipeline': self.map_position_2_pipeline(position, pipeline)
        }

    def change_position(self, position, direction):
        done = False
        if direction > 0:
            for i in range(len(position)):
                if position[i] == 'edge':
                    position[i] = 'cloud'
                    done = True
                    break
        else:
            for i in range(len(position)):
                if position[i] == 'cloud':
                    position[i] = 'edge'
                    done = True
                    break
        return position, done

    def change_single_configuration(self, config_list: list, direction, cur_config, max_config=None):
        cur_index = config_list.index(cur_config)
        max_index = config_list.index(max_config) if max_config is not None else len(config_list) - 1
        min_index = 0
        new_index = cur_index + direction
        new_index = max(new_index, min_index)
        new_index = min(new_index, max_index)

        return config_list[new_index], new_index != cur_index

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
