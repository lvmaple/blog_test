import logging; logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from datetime import datetime

from aiohttp import  web


def index(request):
    headers = {"content-type": "text/html"}
    text = '<h1>Hello {}!</h1>'.format(request.match_info['name'])
    return web.Response(body=text.encode('utf-8'), headers=headers)


async def init(loop):
    app = web.Application(loop=loop)
    # app.router.add_route('GET', '/', index)
    app.router.add_route('GET', '/{name}', index)
    srv = await loop.create_server(app.make_handler(), '127.0.0.1', 8090)
    logging.info(str(datetime.now()) + 'server started at http://127.0.0.1:8090....')
    return srv


loop = asyncio .get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()
