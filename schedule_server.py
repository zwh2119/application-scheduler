import asyncio

from fastapi import FastAPI, BackgroundTasks

from fastapi.routing import APIRoute
from starlette.responses import JSONResponse
from starlette.requests import Request
from fastapi.middleware.cors import CORSMiddleware

from scheduler import Scheduler


class ScheduleServer:
    def __init__(self):
        self.app = FastAPI(routes=[
            APIRoute('/schedule',
                     self.generate_schedule_plan,
                     response_class=JSONResponse,
                     methods=['GET']
                     ),
            APIRoute('/scenario',
                     self.update_scenario,
                     response_class=JSONResponse,
                     methods=['POST']
                     ),
        ], log_level='trace', timeout=6000)

        self.app.add_middleware(
            CORSMiddleware, allow_origins=["*"], allow_credentials=True,
            allow_methods=["*"], allow_headers=["*"],
        )

        self.scheduler = Scheduler()

    # TODO: complete schedule plan generator
    async def generate_schedule_plan(self, request: Request):
        data = await request.json()
        source_id = data['source_id']
        self.scheduler.register_schedule_table(source_id)

        plan = self.scheduler.get_schedule_plan()

        return {'plan': plan}

    # TODO: complete scenario update
    async def update_scenario(self, request: Request):
        data = await request.json()
        self.scheduler.update_scheduler_scenario(data['source_id'], data['scenario'])

        return {'msg': 'scheduler scenario update successfully!'}
