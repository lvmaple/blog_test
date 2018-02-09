import orm, asyncio, sys
from models import User, Blog, Comment


async def test(lp):
    await orm.create_pool(lp, user='www-data', password='www-data', db='app_test')

    # u = User(id='001517376550864ec670315ab9d4a28ad36eb1ee202769b000')
    u = User(name='admin', email='admin@example.com', admin=1, passwd='1234567890', image='about:blank')

    await u.save()
    # orm.destroy_pool


loop = asyncio.get_event_loop()
loop.run_until_complete(test(loop))
loop.close()
sys.exit(0)

