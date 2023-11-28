"""
scheduling parameters

frame resolution
frame rate
frame number
encoding
worker (ip:port)
priority
"""


class Scheduler:
    def __init__(self):
        self.schedule_table = {}

    def register_schedule_table(self, source_id):
        if source_id in self.schedule_table:
            return
        self.schedule_table[source_id] = {}

    def get_schedule_plan(self, info):
        pass

    def update_scheduler_scenario(self, source_id, scenario_data):
        pass
