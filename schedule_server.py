import asyncio

from fastapi import FastAPI, BackgroundTasks

from fastapi.routing import APIRoute
from starlette.responses import JSONResponse
from starlette.requests import Request
from fastapi.middleware.cors import CORSMiddleware


class ScheduleServer:
    def __init__(self):
        self.app = FastAPI(routes=[
            APIRoute('/schedule',
                     self.generate_schedule,
                     response_class=JSONResponse,
                     methods=['GET']
                     ),
            APIRoute('/scenario',
                     self.update_scenario,
                     response_class=JSONResponse,
                     methods=['POST']
                     ),
        ], log_level='trace', timeout=6000)

    def generate_schedule(self):
        pass

    def update_scenario(self):
        pass