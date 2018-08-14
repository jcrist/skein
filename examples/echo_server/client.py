import asyncio
import skein
import sys


async def tcp_echo_client(message, loop, host, port):
    """Generic python tcp echo client"""
    print("Connecting to server at %s:%d" % (host, port))
    reader, writer = await asyncio.open_connection(host, port, loop=loop)

    writer.write(message.encode())
    print('Sent: %r' % message)

    data = await reader.read(100)
    print('Received: %r' % data.decode())

    writer.close()


async def echo_all(app, message):
    """Send and recieve a message from all running echo servers"""
    # Loop through all registered server addresses
    for address in app.kv.get_prefix('address.').values():
        # Parse the host and port from the stored address
        host, port = address.decode().split(':')
        port = int(port)

        # Send the message to the echo server
        await tcp_echo_client(message, loop, host, port)


# Get the application id from the command-line args
app_id = sys.argv[1]

# Connect to the application
app = skein.Client().connect(app_id)

# Send message to every running echo server
loop = asyncio.get_event_loop()
loop.run_until_complete(echo_all(app, 'Hello World!'))
loop.close()
