import logging;
import asyncio, os, json, time
from datetime import datetime

from aiohttp import web
from jinja2 import Environment, FileSystemLoader

import orm
from coroweb import add_routes, add_static

logging.basicConfig(level=logging.INFO)


def init_jinja2(app, **kw):
    logging.info('init jija2....')
    options = dict(
        autoescape=kw.get('autoescape', True),
        block_start_string=kw.get('block_start_string', '{%'),
        block_end_string=kw.get('block_end_string', '%}'),
        variable_start_string=kw.get('variable_start_string', '{{'),
        variable_end_string=kw.get('variable_end_string', '}}'),
        auto_reload=kw.get('auto_reload', True)
    )
    path = kw.get('path', None)
    if path is None:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    logging.info('set jinja2 template path: {!s}'.format(path))
    env = Environment(loader=FileSystemLoader(path), **options)
    filters = kw.get('filters', None)
    if filters is not None:
        for name, f in filters.items():
            env.filters[name] = f
    app['__templating__'] = env


async def logger_factory(app, handler):
    async def logger(request):
        logging.info('Request: {!s} {!s}'.format(request.method, request.path))
        return (await handler(request))

    return logger


async def data_factory(app, handler):
    async def parse_data(request):
        if request.content_type.startswith('application/json'):
            request.__data__ = await request.json()
            logging.info('request json: {!s}'.format(str(request.__data__)))
        elif request.content_type.startswith('application/x-www-form-urlencoded'):
            request.__data__ = await  request.post()
            logging.info('request form: {!s}'.format(request.__data__))
        return (await handler(request))

    return parse_data


async def response_factory(app, handler):
    async def response(request):
        logging.info('Response handler...')
        r = await handler(request)
        if isinstance(r, web.StreamResponse):
            return r
        if isinstance(r, bytes):
            resp = web.Response(body=r)
            resp.content_type = 'application/octet-stream'
            return resp
        if isinstance(r, str):
            if r.startswith('redirect:'):
                return web.HTTPFound(r[9:])
            resp = web.Response(body=r.encode('utf-8'))
            resp.content_type = 'text/html;charset=utf-8'
            return resp
        if isinstance(r, dict):
            template = r.get('__template__')
            if template is None:
                resp = web.Response(
                    body=json.dumps(r, ensure_ascii=False, default=lambda o: o.__dict__).encode('utf-8'))
                resp.content_type = 'application/json;charset=utf-8'
                return resp
            else:
                resp = web.Response(body=app['__templating__'].get_template(template).render(**r).encode('utf-8'))
                resp.content_type = 'text/html;charset=utf-8'
                return resp
        if isinstance(r, int) and 100 < r < 600:
            return web.Response(r)
        if isinstance(r, tuple) and len(r) == 2:
            t, m = r
            if isinstance(t, int) and 100 < t < 600:
                return web.Response(t, str(m))
        # default:
        resp = web.Response(body=str(r).encode('utf-8'))
        resp.content_type = 'text/plain;charset=utf-8'
        return resp

    return response


def datetime_filter(t):
    delta = int(time.time() - t)
    if delta < 60:
        return u'1 minute ago'
    if delta < 3600:
        return u'{!s} minutes ago'.format(delta // 60)
    if delta < 86400:
        return u'{!s} hours ago'.format(delta // 3600)
    if delta < 604800:
        return u'{!s} days ago'.format(delta // 86400)
    dt = datetime.fromtimestamp(t)
    return u'{!s}.{!s}.{!s}'.format(dt.year, dt.month, dt.day)


# def index(request):
#     headers = {"content-type": "text/html"}
#     text = '<h1>Hello {}!</h1>'.format(request.match_info['name'])
#     return web.Response(body=text.encode('utf-8'), headers=headers)
#
#
# async def init(loop):
#     app = web.Application(loop=loop)
#     # app.router.add_route('GET', '/', index)
#     app.router.add_route('GET', '/{name}', index)
#     srv = await loop.create_server(app.make_handler(), '127.0.0.1', 8090)
#     logging.info(str(datetime.now()) + 'server started at http://127.0.0.1:8090....')
#     return srv
async def init(loop):
    await orm.create_pool(loop=loop, host='127.0.0.1', port=3306, user='www-data', password='www-data', db='app_test')
    app = web.Application(loop=loop, middlewares=[logger_factory, response_factory])
    init_jinja2(app, filters=dict(datetime=datetime_filter))
    add_routes(app, 'handlers')
    add_static(app)
    srv = await loop.create_server(app.make_handler(), '127.0.0.1', 8090)
    logging.info(str(datetime.now()) + 'server started at http://127.0.0.1:8090....')
    return srv


loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()
